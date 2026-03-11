import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:PiliPlus/http/init.dart';
import 'package:PiliPlus/models_new/download/bili_download_entry_info.dart';
import 'package:PiliPlus/utils/storage_pref.dart';
import 'package:PiliPlus/utils/extension/file_ext.dart';
import 'package:PiliPlus/utils/extension/string_ext.dart';
import 'package:dio/dio.dart';

import 'package:synchronized/synchronized.dart';

class DownloadChunk {
  final int start;
  final int end;
  int downloaded;

  DownloadChunk({required this.start, required this.end, this.downloaded = 0});

  factory DownloadChunk.fromJson(Map<String, dynamic> json) => DownloadChunk(
    start: json['start'] as int,
    end: json['end'] as int,
    downloaded: json['downloaded'] as int,
  );

  Map<String, dynamic> toJson() => {
    'start': start,
    'end': end,
    'downloaded': downloaded,
  };

  bool get isCompleted => downloaded >= (end - start + 1);
}

class DownloadManager {
  final String url;
  final String path;
  final Map<String, String> headers;
  final int concurrency;
  final void Function(int, int)? onReceiveProgress;
  final void Function([Object? error]) onDone;

  DownloadStatus _status = DownloadStatus.downloading;

  DownloadStatus get status => _status;
  final _cancelToken = CancelToken();
  late Future<void> task;

  DownloadManager({
    required this.url,
    required this.path,
    this.headers = const {},
    this.concurrency = 4,
    required this.onReceiveProgress,
    required this.onDone,
  }) {
    task = _start();
  }

  static const int _persistChunkThresholdBytes = 512 * 1024;
  static const int _persistChunkThresholdMillis = 1000;

  int get _maxRetryCount => Pref.retryCount;

  Duration _retryDelay(int retry) =>
      Duration(milliseconds: (retry <= 0 ? 1 : retry) * Pref.retryDelay);

  bool _isCancelError(Object error) =>
      error is DioException && error.type == DioExceptionType.cancel;

  bool _shouldRetry(Object error) {
    if (error is DioException) {
      if (error.type == DioExceptionType.cancel) {
        return false;
      }
      if (error.type == DioExceptionType.connectionError ||
          error.type == DioExceptionType.connectionTimeout ||
          error.type == DioExceptionType.receiveTimeout ||
          error.type == DioExceptionType.sendTimeout ||
          error.type == DioExceptionType.unknown) {
        return true;
      }
      final status = error.response?.statusCode;
      return status != null &&
          (status == 408 || status == 429 || status >= 500);
    }
    return error is SocketException ||
        error is HttpException ||
        error is HandshakeException ||
        error is TimeoutException;
  }

  Map<String, dynamic> _buildHeaders([Map<String, dynamic>? extra]) => {
    ...headers,
    if (extra != null) ...extra,
  };

  int? _contentRangeTotal(Headers headers) {
    final value = headers.value('content-range');
    if (value == null || !value.contains('/')) {
      return null;
    }
    return int.tryParse(value.split('/').last);
  }

  Future<void> _start() async {
    final file = File(path);
    final chunkFile = File('$path.chunks.json');
    List<DownloadChunk> chunks = const [];
    RandomAccessFile? raf;

    try {
      // 1. 获取文件大小并检查 Range 支持
      // 使用 GET 请求 0-0 字节来获取 Content-Length 并确认 206 Partial Content
      final initRes = await Request.dio.get<ResponseBody>(
        url.http2https,
        options: Options(
          headers: _buildHeaders({'range': 'bytes=0-0'}),
          responseType: ResponseType.stream,
          receiveTimeout: Duration.zero,
          validateStatus: (s) => s != null && (s == 200 || s == 206),
        ),
      );

      final contentRange = _contentRangeTotal(initRes.headers);
      final totalBytes =
          contentRange ??
          int.tryParse(initRes.headers.value('content-length') ?? '');

      final bool supportsRange = initRes.statusCode == 206;

      if (totalBytes == null || !supportsRange || concurrency <= 1) {
        await _startSingleThread(totalBytes);
        return;
      }

      // 2. 准备分片
      if (chunkFile.existsSync()) {
        try {
          final json = jsonDecode(await chunkFile.readAsString()) as List;
          chunks = json
              .map((e) => DownloadChunk.fromJson(e as Map<String, dynamic>))
              .toList();
          if (!_isValidChunks(chunks, totalBytes)) {
            chunks = _createChunks(totalBytes);
          }
        } catch (_) {
          chunks = _createChunks(totalBytes);
        }
      } else {
        chunks = _createChunks(totalBytes);
      }

      // 预先创建完整大小的文件
      if (!file.existsSync() || (await file.length()) != totalBytes) {
        await file.create(recursive: true);
        final raf = await file.open(mode: FileMode.write);
        await raf.truncate(totalBytes);
        await raf.close();
        chunks = _createChunks(totalBytes);
      }

      // 3. 并发下载
      raf = await file.open(mode: FileMode.append);
      final lock = Lock();
      final persistLock = Lock();
      int totalDownloaded = chunks.fold(0, (p, e) => p + e.downloaded);

      Future<void> saveChunks() {
        return persistLock.synchronized(
          () => chunkFile.writeAsString(jsonEncode(chunks), flush: true),
        );
      }

      onReceiveProgress?.call(totalDownloaded, totalBytes);

      final futures = <Future>[];
      final chunkCancelTokens = <CancelToken>[];
      int? lastProgressUpdate;

      for (var i = 0; i < chunks.length; i++) {
        if (chunks[i].isCompleted) continue;

        final chunkCancelToken = CancelToken();
        chunkCancelTokens.add(chunkCancelToken);

        futures.add(
          _downloadChunk(
            chunk: chunks[i],
            raf: raf,
            lock: lock,
            cancelToken: chunkCancelToken,
            onProgress: (received) {
              totalDownloaded += received;
              final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
              if (lastProgressUpdate != now) {
                lastProgressUpdate = now;
                onReceiveProgress?.call(totalDownloaded, totalBytes);
              }
            },
            saveChunks: saveChunks,
          ),
        );
      }

      _cancelToken.whenCancel.then((_) {
        for (final t in chunkCancelTokens) {
          t.cancel();
        }
      });

      if (futures.isNotEmpty) {
        await Future.wait(futures);
      }

      if (_status == DownloadStatus.downloading) {
        _status = DownloadStatus.completed;
        if (chunkFile.existsSync()) await chunkFile.delete();
        onReceiveProgress?.call(totalBytes, totalBytes);
        onDone();
      }
    } catch (e) {
      if (chunks.isNotEmpty && _status != DownloadStatus.completed) {
        try {
          await chunkFile.writeAsString(jsonEncode(chunks), flush: true);
        } catch (_) {}
      }
      if (_status == DownloadStatus.downloading) {
        if (e is DioException && e.type == DioExceptionType.cancel) {
          _status = DownloadStatus.pause;
        } else {
          _status = DownloadStatus.failDownload;
        }
      }
      onDone(e);
    } finally {
      try {
        await raf?.close();
      } catch (_) {}
    }
  }

  List<DownloadChunk> _createChunks(int totalBytes) {
    final chunks = <DownloadChunk>[];
    final chunkSize = (totalBytes / concurrency).ceil();
    for (var i = 0; i < concurrency; i++) {
      final start = i * chunkSize;
      if (start >= totalBytes) break;
      final end = (i + 1) * chunkSize - 1;
      chunks.add(
        DownloadChunk(
          start: start,
          end: end > totalBytes - 1 ? totalBytes - 1 : end,
        ),
      );
    }
    return chunks;
  }

  bool _isValidChunks(List<DownloadChunk> chunks, int totalBytes) {
    if (chunks.isEmpty) {
      return false;
    }
    final sorted = [...chunks]..sort((a, b) => a.start.compareTo(b.start));
    var nextStart = 0;
    for (final chunk in sorted) {
      final length = chunk.end - chunk.start + 1;
      if (length <= 0 ||
          chunk.start != nextStart ||
          chunk.end >= totalBytes ||
          chunk.downloaded < 0 ||
          chunk.downloaded > length) {
        return false;
      }
      nextStart = chunk.end + 1;
    }
    return nextStart == totalBytes;
  }

  Future<void> _downloadChunk({
    required DownloadChunk chunk,
    required RandomAccessFile raf,
    required Lock lock,
    required CancelToken cancelToken,
    required void Function(int) onProgress,
    required Future<void> Function() saveChunks,
  }) async {
    int retry = 0;
    int lastPersisted = chunk.downloaded;
    int lastPersistTs = DateTime.now().millisecondsSinceEpoch;

    Future<void> persist({bool force = false}) async {
      final now = DateTime.now().millisecondsSinceEpoch;
      if (force ||
          chunk.downloaded - lastPersisted >= _persistChunkThresholdBytes ||
          now - lastPersistTs >= _persistChunkThresholdMillis) {
        lastPersisted = chunk.downloaded;
        lastPersistTs = now;
        await saveChunks();
      }
    }

    try {
      while (true) {
        final start = chunk.start + chunk.downloaded;
        if (start > chunk.end) {
          break;
        }

        try {
          final response = await Request.dio.get<ResponseBody>(
            url.http2https,
            options: Options(
              headers: _buildHeaders({'range': 'bytes=$start-${chunk.end}'}),
              responseType: ResponseType.stream,
              receiveTimeout: Duration.zero,
            ),
            cancelToken: cancelToken,
          );

          if (response.statusCode != 206) {
            throw DioException.badResponse(
              statusCode: response.statusCode ?? -1,
              requestOptions: response.requestOptions,
              response: response,
            );
          }

          await for (final data in response.data!.stream) {
            await lock.synchronized(() async {
              await raf.setPosition(chunk.start + chunk.downloaded);
              await raf.writeFrom(data);
            });
            chunk.downloaded += data.length;
            onProgress(data.length);
            await persist();
          }
          retry = 0;
        } catch (e) {
          if (_isCancelError(e)) {
            rethrow;
          }
          if (!_shouldRetry(e) || retry >= _maxRetryCount) {
            rethrow;
          }
          retry++;
          await persist(force: true);
          await Future.delayed(_retryDelay(retry));
        }
      }
    } finally {
      await persist(force: true);
    }
  }

  Future<void> _startSingleThread(int? totalBytesKnown) async {
    int received;
    final file = File(path);
    if (file.existsSync()) {
      received = await file.length();
    } else {
      file.createSync(recursive: true);
      received = 0;
    }

    IOSink sink = file.openWrite(
      mode: received == 0 ? FileMode.writeOnly : FileMode.writeOnlyAppend,
    );

    Future<void> onError(Object e, {bool delete = false}) async {
      try {
        await sink.close();
      } catch (_) {}
      if (_status == DownloadStatus.downloading) {
        if (e is DioException && e.type == DioExceptionType.cancel) {
          _status = DownloadStatus.pause;
        } else {
          _status = DownloadStatus.failDownload;
        }
        if (delete && file.existsSync()) {
          await file.tryDel();
        }
      }
      onDone(e);
    }

    int retry = 0;
    bool initedProgress = false;
    int? totalBytes = totalBytesKnown;

    while (true) {
      Response<ResponseBody> response;
      try {
        response = await Request.dio.get<ResponseBody>(
          url.http2https,
          options: Options(
            headers: _buildHeaders({'range': 'bytes=$received-'}),
            responseType: ResponseType.stream,
            receiveTimeout: Duration.zero,
            validateStatus: (status) =>
                status != null &&
                (status == 416 || (status >= 200 && status < 300)),
          ),
          cancelToken: _cancelToken,
        );
      } on DioException catch (e) {
        if (_isCancelError(e)) {
          await onError(e);
          return;
        }
        if (_shouldRetry(e) && retry < _maxRetryCount) {
          retry++;
          await Future.delayed(_retryDelay(retry));
          continue;
        }
        await onError(e, delete: received == 0);
        return;
      }

      if (response.statusCode == 416) {
        final finalBytes = totalBytes ?? received;
        await sink.close();
        _status = DownloadStatus.completed;
        onReceiveProgress?.call(finalBytes, finalBytes);
        onDone();
        return;
      }

      final data = response.data!;
      if (response.statusCode == 200 && received > 0) {
        await sink.close();
        sink = file.openWrite(mode: FileMode.writeOnly);
        received = 0;
      }

      totalBytes ??=
          _contentRangeTotal(response.headers) ??
          switch (data.contentLength) {
            final length when length > 0 =>
              (response.statusCode == 206) ? length + received : length,
            _ => null,
          };

      if (!initedProgress && totalBytes != null) {
        initedProgress = true;
        onReceiveProgress?.call(received, totalBytes);
      }

      try {
        final onProgress = onReceiveProgress;
        if (onProgress != null && totalBytes != null) {
          int? last;
          await for (final chunk in data.stream) {
            sink.add(chunk);
            received += chunk.length;
            final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
            if (last != now) {
              last = now;
              onProgress.call(received, totalBytes);
            }
          }
        } else {
          await sink.addStream(data.stream);
        }
        await sink.flush();
        retry = 0;
        final finalBytes = totalBytes ?? received;
        await sink.close();
        _status = DownloadStatus.completed;
        onReceiveProgress?.call(finalBytes, finalBytes);
        onDone();
        return;
      } catch (e) {
        if (_isCancelError(e)) {
          await onError(e);
          return;
        }
        if (_shouldRetry(e) && retry < _maxRetryCount) {
          retry++;
          await Future.delayed(_retryDelay(retry));
          continue;
        }
        await onError(e);
        return;
      }
    }
  }

  Future<void> cancel({required bool isDelete}) {
    if (!isDelete && _status == DownloadStatus.downloading) {
      _status = DownloadStatus.pause;
    }
    if (!_cancelToken.isCancelled) {
      _cancelToken.cancel();
    }
    return task;
  }
}
