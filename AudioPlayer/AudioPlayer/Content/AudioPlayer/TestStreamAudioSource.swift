//
//  TestStreamAudioSource.swift
//  AudioPlayer
//
//  Created by Jackson Harper on 10/12/24.
//
import Foundation
import AudioStreaming
import AVFoundation

final class TestStreamAudioSource: NSObject, CoreAudioStreamSource {
    weak var player: AudioPlayer?
    weak var delegate: AudioStreamSourceDelegate?

    var underlyingQueue: DispatchQueue

    var position = 0
    var length = 0

    let buffers: [Data]
    let onReady: () -> Void
    let audioFileHint: AudioFileTypeID

    init(player: AudioPlayer, type: AudioFileTypeID, buffers: [Data], onReady: @escaping () -> Void) {
        self.player = player
        self.audioFileHint = type
        self.buffers = buffers
        self.onReady = onReady
        self.underlyingQueue = player.sourceQueue
    }

    // no-op
    func close() {}

    // no-op
    func suspend() {}

    func resume() {}

    func seek(at _: Int) {
        // The streaming process is started by a seek(0) call from AudioStreaming
        generateData()
    }

    private func generateData() {
        underlyingQueue.asyncAfter(deadline: .now().advanced(by: .milliseconds(100))) {
            for buffer in self.buffers {
                // Debug print raw data
                    print("buffer Data Analysis:")
                    print("Data size: \(buffer.count) bytes")
                    buffer.withUnsafeBytes { rawBufferPointer in
                        let int16Ptr = rawBufferPointer.bindMemory(to: Int16.self)
                        print("First 10 samples as Int16:")
                        for i in 0..<min(10, int16Ptr.count) {
                            print(String(format: "Sample %d: %6d (0x%04X)", i, int16Ptr[i], UInt16(bitPattern: int16Ptr[i])))
                        }
                    }
                self.length += buffer.count
                self.delegate?.dataAvailable(source: self, data: buffer)
            }
            DispatchQueue.main.async {
                self.onReady()
            }
        }
    }
}
