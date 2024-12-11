//
//  PlayerStreamTest.swift
//  AudioStreaming
//
//  Created by Jackson Harper on 25/10/24.
//  Copyright © 2024 Decimal. All rights reserved.
//


import AVFoundation
import Foundation
import XCTest

@testable import AudioStreaming

class PlayerSteamTest: XCTestCase {
    let audioFormat = AVAudioFormat(
        commonFormat: .pcmFormatInt16, sampleRate: 44100, channels: 1, interleaved: true
    )!

    func testPlayerQueueEntriesInitsEmpty() {
        let queue = PlayerQueueEntries()
        
        XCTAssertTrue(queue.isEmpty)
        XCTAssertEqual(queue.count, 0)
        XCTAssertEqual(queue.count(for: .buffering), 0)
        XCTAssertEqual(queue.count(for: .upcoming), 0)
    }
    
    func testPlayerStreamWAVData() {
        let path = Bundle(for: Self.self).path(forResource: "short-counting-to-five-02", ofType: "wav")!
        let pcmData = getLPCMData(wavData: NSData(contentsOfFile: path)!)
//        let data2 = getLPCMData(wavData: NSData(contentsOfFile: path)!)
//        let data3 = getLPCMData(wavData: NSData(contentsOfFile: path)!)

        let expectation = XCTestExpectation(description: "Wait audio to be queued")

        let player = AudioPlayer(configuration: AudioPlayerConfiguration(bufferSizeInSeconds: 1000))
//        let stream = TestStreamAudioSource(player: player, type: kAudioFileWAVEType, buffers: [data1, /* data2 , data3 */]) {
//            expectation.fulfill()
//        }
        let stream =  TestStreamAudioSource(player: player, type: kAudioFileWAVEType, buffers: [pcmData, /* lpcmData */], onReady: {
            print("AUDIO STREAMING SET UP: ", player.duration)
        })
//        let audioFormat = AVAudioFormat(
//            commonFormat: .pcmFormatInt16, sampleRate: 44100, channels: 2, interleaved: false
//        )!

        player.play(source: stream, entryId: UUID().uuidString, format: audioFormat)

        wait(for: [expectation], timeout: 10)
        print("FOUND PLAYER DURATION: ", player.duration)
        XCTAssertGreaterThan(player.duration, 3)
    }
    
    func testPlayerStreamMP3Data() {
        let path = Bundle(for: Self.self).path(forResource: "short-counting-to-five", ofType: "mp3")!
        let data1 = (try? Data(NSData(contentsOfFile: path)))!
        let data2 = (try? Data(NSData(contentsOfFile: path)))!
        let data3 = (try? Data(NSData(contentsOfFile: path)))!

        let expectation = XCTestExpectation(description: "Wait audio to be queued")

        let player = AudioPlayer(configuration: .default)
        let stream = TestStreamAudioSource(player: player, type: kAudioFileMP3Type, buffers: [data1, data2, data3]) {
            expectation.fulfill()
        }
//        let audioFormat = AVAudioFormat(
//            commonFormat: .pcmFormatInt16, sampleRate: 44100, channels: 2, interleaved: false
//        )!

        player.play(source: stream, entryId: UUID().uuidString, format: audioFormat)

        wait(for: [expectation], timeout: 5)
        XCTAssertGreaterThan(player.duration, 3)
    }
}

func getLPCMData(wavData: NSData) -> Data {
    let dataStart = debugPrintWAVHeader(data:  Data(wavData))
    let headerSize = dataStart ?? 0
    let pcmData = Data(bytes: wavData.bytes.advanced(by: headerSize),
                          count: wavData.length - headerSize)
    return pcmData
}


func debugPrintWAVHeader(data: Data) -> Int? {
    guard data.count >= 44 else {
        print("Data too small to be a WAV header")
        return nil
    }
    
    // RIFF Header
    let riffHeader = String(data: data[0..<4], encoding: .ascii) ?? ""
    // Safe byte reading for file size
    let fileSize = data[4..<8].withUnsafeBytes { ptr -> UInt32 in
        var value: UInt32 = 0
        memcpy(&value, ptr.baseAddress, 4)
        return UInt32(littleEndian: value)  // WAV files are little-endian
    }
    let waveHeader = String(data: data[8..<12], encoding: .ascii) ?? ""
    
    print("\nWAV Header Analysis:")
    print("RIFF Header:", riffHeader)
    print("File Size:", fileSize)
    print("WAVE Header:", waveHeader)
    
    // Look for chunks
    var offset: Int = 12  // Skip RIFF header and WAV id
    while offset < data.count - 8 {
        let chunkID = String(data: data[offset..<offset+4], encoding: .ascii) ?? ""
        // Safe byte reading for chunk size
        let chunkSize = data[offset+4..<offset+8].withUnsafeBytes { ptr -> UInt32 in
            var value: UInt32 = 0
            memcpy(&value, ptr.baseAddress, 4)
            return UInt32(littleEndian: value)  // WAV files are little-endian
        }
        
        print("\nChunk Found at offset \(offset):")
        print("  ID:", chunkID)
        print("  Size:", chunkSize)
        
        if chunkID == "data" {
            print("  >>> Data chunk starts at byte \(offset + 8) <<<")
            // Print first few bytes of data for verification
            let dataStart = offset + 8
            let bytesToShow = min(16, data.count - dataStart)
            print("  First \(bytesToShow) bytes of data:", data[dataStart..<dataStart+bytesToShow].map { String(format: "%02X", $0) }.joined(separator: " "))
            return offset + 8
        }
        
        offset += 8 + Int(chunkSize)
        if offset % 2 == 1 { offset += 1 }  // Padding byte if chunk size is odd
    }
    
    return nil
}



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
                self.length += buffer.count
                print("START DATA AVAILABLE")
                self.delegate?.dataAvailable(source: self, data: buffer)
                print("END DATA AVAILABLE")
            }
            DispatchQueue.main.async {
                self.onReady()
            }
        }
    }
}
