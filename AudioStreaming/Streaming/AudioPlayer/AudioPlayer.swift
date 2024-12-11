//
//  Created by Dimitrios Chatzieleftheriou on 01/06/2020.
//  Copyright © 2020 Decimal. All rights reserved.
//

import AVFoundation
import CoreAudio

open class AudioPlayer {
    public weak var delegate: AudioPlayerDelegate?

    public var muted: Bool {
        get { playerContext.muted.value }
        set { playerContext.muted.write { $0 = newValue } }
    }

    /// The volume of the audio
    ///
    /// Defaults to 1.0. Valid ranges are 0.0 to 1.0
    /// The value is restricted from 0.0 to 1.0
    public var volume: Float {
        get { audioEngine.mainMixerNode.outputVolume }
        set { audioEngine.mainMixerNode.outputVolume = min(1.0, max(0.0, newValue)) }
    }

    /// The playback rate of the player.
    ///
    /// The default value is 1.0. Valid ranges are 1/32 to 32.0
    ///
    /// **NOTE:** Setting this to a value of more than `1.0` while playing a live broadcast stream would
    /// result in the audio being exhausted before it could fetch new data.
    public var rate: Float {
        get { rateNode.rate }
        set { rateNode.rate = newValue }
    }

    /// The player's current state.
    public var state: AudioPlayerState {
        playerContext.state.value
    }

    /// Indicates the reason that the player stopped.
    public var stopReason: AudioPlayerStopReason {
        playerContext.stopReason.value
    }

    /// The duration of the audio, in seconds.
    ///
    /// **NOTE** In live audio playback this will be `0.0`
    ///
    /// - Returns: A `Double` value indicating the total duration.
    public var duration: Double {
        guard playerContext.internalState != .pendingNext else { return 0 }
        playerContext.entriesLock.lock()
        let playingEntry = playerContext.audioPlayingEntry
        playerContext.entriesLock.unlock()
        guard let entry = playingEntry else { return 0 }

        let entryDuration = entry.duration()
        let progress = self.progress
        if entryDuration < progress, entryDuration > 0 {
            return progress
        }
        return entryDuration
    }

    /// The progress of the audio playback, in seconds.
    public var progress: Double {
        guard playerContext.internalState != .pendingNext else { return 0 }
        playerContext.entriesLock.lock()
        let playingEntry = playerContext.audioPlayingEntry
        playerContext.entriesLock.unlock()
        guard let entry = playingEntry else { return 0 }
        entry.seekRequest.lock.lock()
        let seekRequested = entry.seekRequest.requested
        let seekTime = entry.seekRequest.time
        entry.seekRequest.lock.unlock()
        if seekRequested {
            return seekTime
        }
        return entry.progress
    }

    public private(set) var customAttachedNodes = [AVAudioNode]()

    /// The current configuration of the player.
    public let configuration: AudioPlayerConfiguration

    /// A Boolean value that indicates whether the audio engine is running.
    /// `true` if the engine is running, otherwise, `false`
    public var isEngineRunning: Bool { audioEngine.isRunning }

    /// The `AVAudioMixerNode` as created by the underlying audio engine
    public var mainMixerNode: AVAudioMixerNode {
        audioEngine.mainMixerNode
    }

    public var frameFiltering: FrameFiltering {
        frameFilterProcessor
    }

    /// An `AVAudioFormat` object for the canonical audio stream
    private var outputAudioFormat: AVAudioFormat = .init(commonFormat: .pcmFormatFloat32, sampleRate: 44100.0, channels: 2, interleaved: true)!

    /// Keeps track of the player's state before being paused.
    private var stateBeforePaused: InternalState = .initial

    /// The underlying `AVAudioEngine` object
    private let audioEngine: AVAudioEngine
    /// An `AVAudioUnit` object that represents the audio player
    private(set) var player = AVAudioUnit()
    /// An `AVAudioUnitTimePitch` that controls the playback rate of the audio engine
    private let rateNode = AVAudioUnitTimePitch()

    /// An object representing the context of the audio render.
    /// Holds the audio buffer and in/out lists as required by the audio rendering
    private let rendererContext: AudioRendererContext
    /// An object representing the context of the player.
    /// Holds the player's state, current playing and reading entries.
    private let playerContext: AudioPlayerContext

    private let fileStreamProcessor: AudioFileStreamProcessor
    private let playerRenderProcessor: AudioPlayerRenderProcessor
    private let frameFilterProcessor: FrameFilterProcessor

    private let serializationQueue: DispatchQueue
    public let sourceQueue: DispatchQueue

    private let entryProvider: AudioEntryProviding

    var entriesQueue: PlayerQueueEntries

    public init(configuration: AudioPlayerConfiguration = .default) {
        self.configuration = configuration.normalizeValues()
        let engine = AVAudioEngine()
        audioEngine = engine
        rendererContext = AudioRendererContext(configuration: configuration, outputAudioFormat: outputAudioFormat)
        playerContext = AudioPlayerContext()
        entriesQueue = PlayerQueueEntries()

        serializationQueue = DispatchQueue(label: "streaming.core.queue", qos: .userInitiated)
        sourceQueue = DispatchQueue(label: "source.queue", qos: .default)

        entryProvider = AudioEntryProvider(
            networkingClient: NetworkingClient(),
            underlyingQueue: sourceQueue,
            outputAudioFormat: outputAudioFormat
        )

        fileStreamProcessor = AudioFileStreamProcessor(
            playerContext: playerContext,
            rendererContext: rendererContext,
            outputAudioFormat: outputAudioFormat.basicStreamDescription
        )

        playerRenderProcessor = AudioPlayerRenderProcessor(
            playerContext: playerContext,
            rendererContext: rendererContext,
            outputAudioFormat: outputAudioFormat.basicStreamDescription
        )

        frameFilterProcessor = FrameFilterProcessor(
            mixerNodeProvider: {
                engine.mainMixerNode
            }
        )
        configPlayerContext()
        configPlayerNode()
        setupEngine()
    }

    deinit {
        playerContext.audioPlayingEntry?.close()
        clearQueue()
        rendererContext.clean()
    }

    // MARK: Public

    /// Starts the audio playback for the given URL
    ///
    /// - parameter url: A `URL` specifying the audio context to be played
    public func play(url: URL) {
        play(url: url, headers: [:])
    }

    /// Starts the audio playback for the given URL
    ///
    /// - parameter url: A `URL` specifying the audio context to be played.
    /// - parameter headers: A `Dictionary` specifying any additional headers to be pass to the network request.
    public func play(url: URL, headers: [String: String]) {
        let audioEntry = entryProvider.provideAudioEntry(url: url, headers: headers)
        play(audioEntry: audioEntry)
    }

    /// Starts the audio playback for the supplied stream
    ///
    /// - parameter source: A `CoreAudioStreamSource` that will providing streaming data
    /// - parameter entryId: A `String` that provides a unique id for this item
    /// - parameter format: An `AVAudioFormat` the format of this audio source
    public func play(source: CoreAudioStreamSource, entryId: String, format: AVAudioFormat) {
        let audioEntry = AudioEntry(source: source, entryId: AudioEntryId(id: entryId), outputAudioFormat: format)
        
        if source.audioFileHint == kAudioFileWAVEType {
            setupInitialPCMProperties(entry: audioEntry)
        }
        
        play(audioEntry: audioEntry)
    }

    private func play(audioEntry: AudioEntry) {
        audioEntry.delegate = self

        checkRenderWaitingAndNotifyIfNeeded()
        serializationQueue.sync {
            clearQueue()
            entriesQueue.enqueue(item: audioEntry, type: .upcoming)
            playerContext.setInternalState(to: .pendingNext)
            do {
                try self.startEngineIfNeeded()
            } catch {
                self.raiseUnexpected(error: .audioSystemError(.engineFailure))
            }
        }

        sourceQueue.async { [weak self] in
            guard let self = self else { return }
            self.processSource()
        }
    }

    public func playNextInQueue() {
        checkRenderWaitingAndNotifyIfNeeded()
        serializationQueue.sync {
            if entriesQueue.count(for: .upcoming) > 0 {
                playerContext.setInternalState(to: .pendingNext)
            }
            do {
                try self.startEngineIfNeeded()
            } catch {
                self.raiseUnexpected(error: .audioSystemError(.engineFailure))
            }
        }

        sourceQueue.async { [weak self] in
            guard let self = self else { return }
            self.processSource()
        }
    }

    /// Queues the specified URL
    ///
    /// - Parameter url: A `URL` specifying the audio content to be played.
    public func queue(url: URL) {
        queue(url: url, headers: [:])
    }

    /// Queues the specified URLs
    ///
    /// - Parameter url: A `URL` specifying the audio content to be played.
    public func queue(urls: [URL]) {
        queue(urls: urls, headers: [:])
    }

    public func queue(url: URL, after afterUrl: URL) {
        queue(url: url, headers: [:], after: afterUrl)
    }

    /// Queues the specified audio stream
    ///
    /// - parameter source: A `CoreAudioStreamSource` that will providing streaming data
    /// - parameter entryId: A `String` that provides a unique id for this item
    /// - parameter format: An `AVAudioFormat` the format of this audio source
    public func queue(source: CoreAudioStreamSource, entryId: String, format: AVAudioFormat) {
        let audioEntry = AudioEntry(source: source, entryId: AudioEntryId(id: entryId), outputAudioFormat: format)
        queue(audioEntry: audioEntry)
    }

    public func removeFromQueue(url: URL) {
        serializationQueue.sync {
            if let item = entriesQueue.items(type: .upcoming).first(where: { $0.id.id == url.absoluteString }) {
                entriesQueue.remove(item: item, type: .upcoming)

                if playerContext.audioPlayingEntry?.id.id == item.id.id {
                    stop(clearQueue: false)
                }
            }
        }
        checkRenderWaitingAndNotifyIfNeeded()
        sourceQueue.async { [weak self] in
            self?.processSource()
        }
    }

    /// Queues the specified URL
    ///
    /// - Parameter url: A `URL` specifying the audio content to be played.
    /// - parameter headers: A `Dictionary` specifying any additional headers to be pass to the network request.
    public func queue(url: URL, headers: [String: String], after afterUrl: URL? = nil) {
        let audioEntry = entryProvider.provideAudioEntry(url: url, headers: headers)
        queue(audioEntry: audioEntry, after: afterUrl)
    }

    /// Queues the specified URLs
    ///
    /// - Parameter url: A array of `URL`s specifying the audio content to be played.
    /// - parameter headers: A `Dictionary` specifying any additional headers to be pass to the network request.
    public func queue(urls: [URL], headers: [String: String]) {
        serializationQueue.sync {
            for url in urls {
                let audioEntry = entryProvider.provideAudioEntry(url: url, headers: headers)
                audioEntry.delegate = self
                entriesQueue.enqueue(item: audioEntry, type: .upcoming)
            }
        }
        checkRenderWaitingAndNotifyIfNeeded()
        sourceQueue.async { [weak self] in
            self?.processSource()
        }
    }

    private func queue(audioEntry: AudioEntry, after afterUrl: URL? = nil) {
        serializationQueue.sync {
            audioEntry.delegate = self
            if let afterUrl = afterUrl {
                if let afterUrlEntry = entriesQueue.items(type: .upcoming).first(where: { $0.id.id == afterUrl.absoluteString }) {
                    entriesQueue.insert(item: audioEntry, type: .upcoming, after: afterUrlEntry)
                }
            } else {
                entriesQueue.enqueue(item: audioEntry, type: .upcoming)
            }
        }
        checkRenderWaitingAndNotifyIfNeeded()
        sourceQueue.async { [weak self] in
            self?.processSource()
        }
    }

    /// Stops the audio playback
    public func stop(clearQueue: Bool = true) {
        guard playerContext.internalState != .stopped else { return }

        serializationQueue.sync {
            stopEngine(reason: .userAction)
        }
        checkRenderWaitingAndNotifyIfNeeded()
        sourceQueue.async { [weak self] in
            guard let self = self else { return }
            self.playerContext.audioReadingEntry?.delegate = nil
            self.playerContext.audioReadingEntry?.close()
            if let playingEntry = self.playerContext.audioPlayingEntry {
                self.processFinishPlaying(entry: playingEntry, with: nil)
            }

            if clearQueue {
                self.clearQueue()
            }
            self.playerContext.entriesLock.lock()
            self.playerContext.audioReadingEntry = nil
            self.playerContext.audioPlayingEntry = nil
            self.playerContext.entriesLock.unlock()

            self.processSource()
        }
    }

    /// Pauses the audio playback
    public func pause() {
        if playerContext.internalState != .paused, playerContext.internalState.contains(.running) {
            stateBeforePaused = playerContext.internalState
            playerContext.setInternalState(to: .paused)
            serializationQueue.sync {
                pauseEngine()
            }
            playerContext.audioPlayingEntry?.suspend()
            sourceQueue.async { [weak self] in
                self?.processSource()
            }
        }
    }

    /// Resumes the audio playback, if previous paused
    public func resume() {
        guard playerContext.internalState == .paused else { return }
        playerContext.setInternalState(to: stateBeforePaused)
        serializationQueue.sync {
            do {
                try startEngine()
            } catch {
                Logger.debug("resuming audio engine failed: %@", category: .generic, args: error.localizedDescription)
            }
            if let playingEntry = playerContext.audioReadingEntry {
                if playingEntry.seekRequest.requested {
                    rendererContext.resetBuffers()
                }
                playingEntry.resume()
            }
            startPlayer(resetBuffers: false)
        }
    }

    /// Seeks the audio to the specified time.
    /// - Parameter time: A `Double` value specifying the time of the requested seek in seconds
    public func seek(to time: Double) {
        guard let playingEntry = playerContext.audioPlayingEntry else {
            return
        }
        playingEntry.seekRequest.lock.lock()
        let alreadyRequestedToSeek = playingEntry.seekRequest.requested
        playingEntry.seekRequest.requested = true
        playingEntry.seekRequest.time = time
        playingEntry.seekRequest.lock.unlock()

        if !alreadyRequestedToSeek {
            playingEntry.seekRequest.version.write { version in
                version += 1
            }
            playingEntry.suspend()
            checkRenderWaitingAndNotifyIfNeeded()
            sourceQueue.async { [weak self] in
                self?.processSource()
            }
        }
    }

    /// Attaches the given `AVAudioNode` to the engine
    /// - Note: The node will be added after the default rate node
    /// - Parameter node: An instance of `AVAudioNode`
    public func attach(node: AVAudioNode) {
        attach(nodes: [node])
    }

    /// Attaches the given `AVAudioNode`s to the engine
    /// - Note: The nodes will be added after the default rate node
    /// - Parameter node: An array of `AVAudioNode` instances
    public func attach(nodes: [AVAudioNode]) {
        for node in nodes {
            customAttachedNodes.append(node)
        }
        nodes.forEach(audioEngine.attach)

        reattachCustomNodes()
    }

    /// Detaches the given `AVAudioNode` from the engine
    /// - Parameter node: An instance of `AVAudioNode`
    public func detach(node: AVAudioNode) {
        guard customAttachedNodes.contains(node) else {
            return
        }
        customAttachedNodes.removeAll(where: { $0 == node })
        audioEngine.detach(node)
        reattachCustomNodes()
    }

    /// Detaches the given `AVAudioNode`s from the engine
    /// - Parameter node: An array of `AVAudioNode` instances
    public func detachCustomAttachedNodes() {
        for node in customAttachedNodes {
            audioEngine.detach(node)
        }
        attachAndConnectDefaultNodes()
    }

    // MARK: Private

    /// Setups the audio engine with manual rendering mode.
    private func setupEngine() {
        do {
            // audio engine must be stop before enabling manualRendering mode.
            audioEngine.stop()
            playerRenderProcessor.renderBlock = audioEngine.manualRenderingBlock

            try audioEngine.enableManualRenderingMode(.realtime,
                                                      format: outputAudioFormat,
                                                      maximumFrameCount: maxFramesPerSlice)

            let inputBlock = { [playerRenderProcessor] frameCount -> UnsafePointer<AudioBufferList>? in
                playerRenderProcessor.inRender(inNumberFrames: frameCount)
            }

            let success = audioEngine.inputNode.setManualRenderingInputPCMFormat(outputAudioFormat,
                                                                                 inputBlock: inputBlock)
            guard success else {
                assertionFailure("failure setting manual rendering mode")
                return
            }
            attachAndConnectDefaultNodes()

            audioEngine.prepare()
            try audioEngine.start()
        } catch {
            Logger.error("⚠️ error setting up audio engine: %@", category: .generic, args: error.localizedDescription)
        }
    }

    /// Creates and configures an `AVAudioUnit` with an output configuration
    /// and assigns it to the `player` variable.
    private func configPlayerNode() {
        AVAudioUnit.createAudioUnit(with: UnitDescriptions.output) { [weak self] result in
            guard let self = self else { return }
            switch result {
            case let .success(unit):
                self.player = unit
                self.playerRenderProcessor.attachCallback(on: unit, audioFormat: self.outputAudioFormat)
            case let .failure(error):
                assertionFailure("couldn't create player unit: \(error)")
                self.raiseUnexpected(error: .audioSystemError(.playerNotFound))
            }
        }
    }

    /// Attaches callbacks to the `playerContext` and `renderProcessor`.
    private func configPlayerContext() {
        playerContext.stateChanged = { [weak self] oldValue, newValue in
            guard let self = self else { return }
            asyncOnMain {
                self.delegate?.audioPlayerStateChanged(player: self, with: newValue, previous: oldValue)
            }
        }

        playerRenderProcessor.audioFinishedPlaying = { [weak self] entry in
            guard let self = self else { return }
            self.serializationQueue.sync {
                let nextEntry = self.entriesQueue.dequeue(type: .buffering)
                self.processFinishPlaying(entry: entry, with: nextEntry)
            }
            self.sourceQueue.async {
                self.processSource()
            }
        }

        fileStreamProcessor.fileStreamCallback = { [weak self] effect in
            guard let self = self else { return }
            switch effect {
            case .processSource:
                self.sourceQueue.async {
                    self.processSource()
                }
            case let .raiseError(error):
                self.raiseUnexpected(error: error)
            }
        }
    }

    /// Attaches and connect nodes to the `AudioEngine`.
    private func attachAndConnectDefaultNodes() {
        audioEngine.attach(rateNode)

        audioEngine.connect(audioEngine.inputNode, to: rateNode, format: nil)
        audioEngine.connect(rateNode, to: audioEngine.mainMixerNode, format: nil)
    }

    private func reattachCustomNodes() {
        audioEngine.connect(audioEngine.inputNode, to: rateNode, format: nil)

        if !customAttachedNodes.isEmpty {
            if let first = customAttachedNodes.first {
                audioEngine.connect(rateNode, to: first, format: nil)
            }
            for index in 0 ..< customAttachedNodes.count - 1 {
                let current = customAttachedNodes[index]
                let next = customAttachedNodes[index + 1]
                let format = current.inputFormat(forBus: 0)
                audioEngine.connect(current, to: next, format: format)
            }
            if let last = customAttachedNodes.last {
                audioEngine.connect(last, to: audioEngine.mainMixerNode, format: nil)
            }
        } else {
            audioEngine.connect(rateNode, to: audioEngine.mainMixerNode, format: nil)
        }
    }

    /// Starts the engine, if not already running.
    ///
    /// - Throws: An `Error` when failed to start the engine.
    private func startEngineIfNeeded() throws {
        guard !isEngineRunning else {
            Logger.debug("engine already running 🛵", category: .generic)
            return
        }
        try startEngine()
    }

    /// Force starts the engine
    ///
    /// - Throws: An `Error` when failed to start the engine.
    private func startEngine() throws {
        try audioEngine.start()
        Logger.debug("engine started 🛵", category: .generic)
    }

    /// Pauses the audio engine and stops the player's hardware
    private func pauseEngine() {
        guard isEngineRunning else { return }
        audioEngine.reset()
        audioEngine.pause()
        player.auAudioUnit.stopHardware()
        Logger.debug("engine paused ⏸", category: .generic)
    }

    /// Stops the audio engine and the player's hardware
    ///
    /// - parameter reason: A value of `AudioPlayerStopReason` indicating the reason the engine stopped.
    private func stopEngine(reason: AudioPlayerStopReason) {
        audioEngine.stop()
        player.auAudioUnit.stopHardware()
        rendererContext.resetBuffers()
        playerContext.setInternalState(to: .stopped)
        playerContext.stopReason.write { $0 = reason }
        Logger.debug("engine stopped 🛑", category: .generic)
    }

    /// Starts the audio player, resetting the buffers if requested
    ///
    /// - parameter resetBuffers: A `Bool` value indicating if the buffers should be reset, prior starting the player.
    private func startPlayer(resetBuffers: Bool) {
        if resetBuffers {
            rendererContext.resetBuffers()
        }
        do {
            try startEngineIfNeeded()
            try player.auAudioUnit.allocateRenderResources()
            try player.auAudioUnit.startHardware()
        } catch {
            stopEngine(reason: .error)
            raiseUnexpected(error: .audioSystemError(.playerStartError))
        }
    }

    /// Processing the `playerContext` state to ensure correct behavior of playing/stop/seek
    private func processSource() {
        dispatchPrecondition(condition: .onQueue(sourceQueue))

        guard playerContext.internalState != .paused else { return }

        if playerContext.internalState == .pendingNext {
            let entry = entriesQueue.dequeue(type: .upcoming)
            playerContext.setInternalState(to: .waitingForData)
            setCurrentReading(entry: entry, startPlaying: true, shouldClearQueue: true)
            rendererContext.resetBuffers()
        } else if let playingEntry = playerContext.audioPlayingEntry,
                  playingEntry.seekRequest.requested,
                  playingEntry != playerContext.audioReadingEntry
        {
            playingEntry.audioStreamState.processedDataFormat = false
            playingEntry.reset()
            if let readingEntry = playerContext.audioReadingEntry {
                readingEntry.delegate = nil
                readingEntry.close()
            }
            if configuration.flushQueueOnSeek {
                playerContext.setInternalState(to: .waitingForDataAfterSeek)
                setCurrentReading(entry: playingEntry, startPlaying: true, shouldClearQueue: true)
            } else {
                entriesQueue.requeueBufferingEntries { audioEntry in
                    audioEntry.reset()
                }
                playerContext.setInternalState(to: .waitingForDataAfterSeek)
                setCurrentReading(entry: playingEntry, startPlaying: true, shouldClearQueue: false)
            }

        } else if playerContext.audioReadingEntry == nil {
            if entriesQueue.count(for: .upcoming) > 0 {
                let entry = entriesQueue.dequeue(type: .upcoming)
                let shouldStartPlaying = playerContext.audioPlayingEntry == nil
                playerContext.setInternalState(to: .waitingForData)
                setCurrentReading(entry: entry, startPlaying: shouldStartPlaying, shouldClearQueue: false)
            } else if playerContext.audioPlayingEntry == nil {
                if playerContext.internalState != .stopped {
                    stopEngine(reason: .eof)
                }
            }
        }

        if let playingEntry = playerContext.audioPlayingEntry,
           playingEntry.audioStreamState.processedDataFormat,
           playingEntry.calculatedBitrate() > 0.0
        {
            let currSeekVersion = playingEntry.seekRequest.version.value
            playingEntry.seekRequest.lock.lock()
            let originalSeekToTimeRequested = playingEntry.seekRequest.requested
            playingEntry.seekRequest.lock.unlock()

            if originalSeekToTimeRequested, playerContext.audioReadingEntry === playingEntry {
                processSeekTime()

                let version = playingEntry.seekRequest.version.value
                if currSeekVersion == version {
                    playingEntry.seekRequest.lock.lock()
                    playingEntry.seekRequest.requested = false
                    playingEntry.seekRequest.lock.unlock()
                }
            }
        }
    }

    private func processSeekTime() {
        assert(playerContext.audioReadingEntry === playerContext.audioPlayingEntry,
               "reading and playing entry must be the same")
        fileStreamProcessor.processSeek()
    }

    private func setCurrentReading(entry: AudioEntry?, startPlaying: Bool, shouldClearQueue: Bool) {
        guard let entry = entry else { return }
        Logger.debug("Setting current reading entry to: %@", category: .generic, args: entry.debugDescription)
        if startPlaying {
            rendererContext.fillSilenceAudioBuffer()
        }

        fileStreamProcessor.closeFileStreamIfNeeded()

        if let readingEntry = playerContext.audioReadingEntry {
            readingEntry.delegate = nil
            readingEntry.close()
        }

        entry.delegate = self
        entry.seek(at: 0)
        playerContext.entriesLock.lock()
        playerContext.audioReadingEntry = entry
        playerContext.entriesLock.unlock()

        if startPlaying {
            if shouldClearQueue {
                clearQueue()
            }
            processFinishPlaying(entry: playerContext.audioPlayingEntry, with: entry)
            startPlayer(resetBuffers: true)
        } else {
            entriesQueue.enqueue(item: entry, type: .buffering)
        }
    }

    private func processFinishPlaying(entry: AudioEntry?, with nextEntry: AudioEntry?) {
        let playingEntry = playerContext.entriesLock.withLock { playerContext.audioPlayingEntry }
        guard entry == playingEntry else { return }

        let isPlayingSameItemProbablySeek = playerContext.audioPlayingEntry === nextEntry

        if let nextEntry = nextEntry {
            if !isPlayingSameItemProbablySeek {
                nextEntry.lock.withLock {
                    nextEntry.seekTime = 0
                }
                nextEntry.seekRequest.lock.withLock {
                    nextEntry.seekRequest.requested = false
                }
            }
            playerContext.entriesLock.lock()
            playerContext.audioPlayingEntry = nextEntry
            let playingQueueEntryId = playerContext.audioPlayingEntry?.id ?? AudioEntryId(id: "")
            playerContext.entriesLock.unlock()

            if let entry = entry, !isPlayingSameItemProbablySeek {
                let entryId = entry.id
                let progressInFrames = entry.progressInFrames()
                let progress = Double(progressInFrames) / outputAudioFormat.basicStreamDescription.mSampleRate
                let duration = entry.duration()
                asyncOnMain { [weak self] in
                    guard let self else { return }
                    self.delegate?.audioPlayerDidFinishPlaying(
                        player: self,
                        entryId: entryId,
                        stopReason: self.stopReason,
                        progress: progress,
                        duration: duration
                    )
                }
            }
            if !isPlayingSameItemProbablySeek {
                playerContext.setInternalState(to: .waitingForData)

                asyncOnMain { [weak self] in
                    guard let self = self else { return }
                    self.delegate?.audioPlayerDidStartPlaying(player: self, with: playingQueueEntryId)
                }
            }
        } else {
            playerContext.entriesLock.lock()
            playerContext.audioPlayingEntry = nil
            playerContext.entriesLock.unlock()
            if let entry = entry, !isPlayingSameItemProbablySeek {
                let entryId = entry.id
                let progressInFrames = entry.progressInFrames()
                let progress = Double(progressInFrames) / outputAudioFormat.basicStreamDescription.mSampleRate
                let duration = entry.duration()

                sourceQueue.async { [weak self] in
                    guard let self else { return }
                    self.processSource()
                    asyncOnMain {
                        self.delegate?.audioPlayerDidFinishPlaying(
                            player: self,
                            entryId: entryId,
                            stopReason: self.stopReason,
                            progress: progress,
                            duration: duration
                        )
                    }
                }
            }
        }
        sourceQueue.async { [weak self] in
            self?.processSource()
        }
        checkRenderWaitingAndNotifyIfNeeded()
    }

    /// Clears pending queues and informs the delegate
    private func clearQueue() {
        let pendingItems = entriesQueue.pendingEntriesId()
        entriesQueue.removeAll()
        if !pendingItems.isEmpty {
            asyncOnMain { [weak self] in
                guard let self = self else { return }
                self.delegate?.audioPlayerDidCancel(player: self, queuedItems: pendingItems)
            }
        }
    }

    /// Signals the packet process
    private func checkRenderWaitingAndNotifyIfNeeded() {
        if rendererContext.waiting.value {
            rendererContext.packetsSemaphore.signal()
        }
    }

    private func raiseUnexpected(error: AudioPlayerError) {
        playerContext.setInternalState(to: .error)
        asyncOnMain { [weak self] in
            guard let self = self else { return }
            self.delegate?.audioPlayerUnexpectedError(player: self, error: error)
        }
        Logger.error("Error: %@", category: .generic, args: error.localizedDescription)
    }

    func setupPCMProperties(entry: AudioEntry) {
        // Set up basic PCM stream description
        var streamDescription = AudioStreamBasicDescription()
        streamDescription.mSampleRate = 44100
        streamDescription.mFormatID = kAudioFormatLinearPCM
        streamDescription.mFormatFlags = kAudioFormatFlagIsSignedInteger | kAudioFormatFlagIsPacked
        streamDescription.mBitsPerChannel = 16
        streamDescription.mChannelsPerFrame = 2
        streamDescription.mFramesPerPacket = 1
        streamDescription.mBytesPerFrame = 4    // (16 bits * 2 channels) / 8 bits per byte
        streamDescription.mBytesPerPacket = 4   // same as mBytesPerFrame for PCM
        streamDescription.mReserved = 0

        let headerSize: UInt64 = 0 // 44  // WAV header size
        let totalBytes = UInt64(entry.length)
        let audioDataBytes = totalBytes - headerSize

        // Calculate packet count (same as frame count for PCM)
        let bytesPerFrame = UInt64(streamDescription.mBytesPerFrame)
        let packetCount = audioDataBytes / bytesPerFrame

        
        // Set the audio format
        entry.audioStreamFormat = streamDescription
        entry.audioStreamState.processedDataFormat = true
        
        entry.audioStreamState.processedDataFormat = true
        entry.audioStreamState.dataOffset = 0
        
        entry.audioStreamState.dataByteCount = UInt64(entry.length)
        entry.audioStreamState.dataPacketOffset = 0
        entry.audioStreamState.dataPacketCount = Double(packetCount)
        // Mark as ready to produce packets
        // entry.audioStreamState.readyToProducePackets = true
        
        print("""
            PCM Properties Set:
            Sample Rate: \(streamDescription.mSampleRate)
            Channels: \(streamDescription.mChannelsPerFrame)
            Bits per Channel: \(streamDescription.mBitsPerChannel)
            Bytes per Frame: \(streamDescription.mBytesPerFrame)
            Total Frames: \(packetCount)
            Duration: \(Double(packetCount) / streamDescription.mSampleRate) seconds
            """)
    }

    // Initial setup with basic format info
    func setupInitialPCMProperties(entry: AudioEntry) {
        var streamDescription = AudioStreamBasicDescription()
        streamDescription.mSampleRate = 44100
        streamDescription.mFormatID = kAudioFormatLinearPCM
        streamDescription.mFormatFlags = kAudioFormatFlagIsFloat | kAudioFormatFlagIsPacked
        streamDescription.mBitsPerChannel = 32
        streamDescription.mChannelsPerFrame = 2
        streamDescription.mFramesPerPacket = 1
        streamDescription.mBytesPerFrame =  8    // 16-bit (2 bytes) × 2 channels
        streamDescription.mBytesPerPacket =  8   // Same as mBytesPerFrame for PCM

        print("""
        PCM Format Setup (matching input WAV):
        Sample Rate: \(streamDescription.mSampleRate) Hz
        Channels: \(streamDescription.mChannelsPerFrame)
        Bits per Channel: \(streamDescription.mBitsPerChannel)
        Bytes per Frame: \(streamDescription.mBytesPerFrame)
        Format Flags: \(String(format: "0x%08X", streamDescription.mFormatFlags))
        Sample Format: 16-bit Signed Integer PCM
        """)

        entry.audioStreamFormat = streamDescription
        entry.audioStreamState.processedDataFormat = true
    }

    // Update running totals when new data arrives
    func updatePCMProperties(entry: AudioEntry, newDataSize: Int) {
        let bytesPerFrame = (entry.audioStreamFormat.mBitsPerChannel / 8) * entry.audioStreamFormat.mChannelsPerFrame
        let newPacketCount = Double(newDataSize) / Double(bytesPerFrame)

        entry.audioStreamState.dataByteCount = UInt64(newDataSize)
        entry.audioStreamState.dataPacketCount = newPacketCount
        
        print("""
            Updated PCM Data Info:
            New data size: \(newDataSize) bytes
            Bytes per frame: \(bytesPerFrame) (\(entry.audioStreamFormat.mBitsPerChannel/8) bytes/sample × \(entry.audioStreamFormat.mChannelsPerFrame) channels)
            New frames: \(newPacketCount)
            Total audio bytes: \(entry.audioStreamState.dataByteCount)
            Total frames: \(entry.audioStreamState.dataPacketCount)
            Duration: \(Double(entry.audioStreamState.dataPacketCount) / entry.audioStreamFormat.mSampleRate) seconds
            """)
    }

    func setupAudioConverter(entry: AudioEntry) -> AudioConverterRef? {
        // Source format (16-bit signed integer PCM MONO)
        var inputFormat = AudioStreamBasicDescription()
        inputFormat.mSampleRate = 44100
        inputFormat.mFormatID = kAudioFormatLinearPCM
        inputFormat.mFormatFlags = kAudioFormatFlagIsSignedInteger | kAudioFormatFlagIsPacked
        inputFormat.mBitsPerChannel = 16
        inputFormat.mChannelsPerFrame = 1  // MONO input
        inputFormat.mFramesPerPacket = 1
        inputFormat.mBytesPerFrame = 2    // (16 bits * 1 channel) / 8 bits per byte
        inputFormat.mBytesPerPacket = 2

        // Destination format (32-bit float PCM STEREO)
        var outputFormat = AudioStreamBasicDescription()
        outputFormat.mSampleRate = 44100
        outputFormat.mFormatID = kAudioFormatLinearPCM
        outputFormat.mFormatFlags = kAudioFormatFlagIsFloat | kAudioFormatFlagIsPacked
        outputFormat.mBitsPerChannel = 32
        outputFormat.mChannelsPerFrame = 2  // STEREO output
        outputFormat.mFramesPerPacket = 1
        outputFormat.mBytesPerFrame = 8    // (32 bits * 2 channels) / 8 bits per byte
        outputFormat.mBytesPerPacket = 8

        var converter: AudioConverterRef?
        let status = AudioConverterNew(&inputFormat, &outputFormat, &converter)
        
        if status != noErr {
            print("""
            Converter creation failed:
            Status: \(status)
            Input format: \(inputFormat.mChannelsPerFrame) channels, \(inputFormat.mBytesPerFrame) bytes/frame
            Output format: \(outputFormat.mChannelsPerFrame) channels, \(outputFormat.mBytesPerFrame) bytes/frame
            """)
            return nil
        }
        
        print("""
        Audio Converter Setup:
        Mono -> Stereo conversion
        Input: 16-bit Integer PCM Mono (\(inputFormat.mBytesPerFrame) bytes/frame)
        Output: 32-bit Float PCM Stereo (\(outputFormat.mBytesPerFrame) bytes/frame)
        """)
        
        return converter
    }
    
    // Conversion function
    func convertToFloat32(entry: AudioEntry, _ inputData: Data) -> Data? {
        // Source format (16-bit signed integer PCM)
         var inputFormat = AudioStreamBasicDescription()
         inputFormat.mSampleRate = 44100
         inputFormat.mFormatID = kAudioFormatLinearPCM
         inputFormat.mFormatFlags = kAudioFormatFlagIsSignedInteger | kAudioFormatFlagIsPacked
         inputFormat.mBitsPerChannel = 16
         inputFormat.mChannelsPerFrame = 2
         inputFormat.mFramesPerPacket = 1
         inputFormat.mBytesPerFrame = 4    // (16 bits * 2 channels) / 8 bits per byte
         inputFormat.mBytesPerPacket = 4   // same as mBytesPerFrame for PCM

         // Destination format (32-bit float PCM)
         var outputFormat = AudioStreamBasicDescription()
         outputFormat.mSampleRate = 44100
         outputFormat.mFormatID = kAudioFormatLinearPCM
         outputFormat.mFormatFlags = kAudioFormatFlagIsFloat | kAudioFormatFlagIsPacked
         outputFormat.mBitsPerChannel = 32
         outputFormat.mChannelsPerFrame = 2
         outputFormat.mFramesPerPacket = 1
         outputFormat.mBytesPerFrame = 8    // (32 bits * 2 channels) / 8 bits per byte
         outputFormat.mBytesPerPacket = 8   // same as mBytesPerFrame for PCM

         // Debug print formats before conversion
         print("\nInput Format:")
         print("Sample Rate: \(inputFormat.mSampleRate)")
         print("Channels: \(inputFormat.mChannelsPerFrame)")
         print("Bits per Channel: \(inputFormat.mBitsPerChannel)")
         print("Bytes per Frame: \(inputFormat.mBytesPerFrame)")
         print("Format Flags: \(String(format: "0x%08X", inputFormat.mFormatFlags))")

         var converter: AudioConverterRef?
         let status = AudioConverterNew(&inputFormat, &outputFormat, &converter)
         
         if status != noErr {
             print("Failed to create converter with status: \(status) (0x\(String(format: "%08X", status)))")
             return nil
         }
            
        
        let framesPerBuffer = inputData.count / Int(inputFormat.mBytesPerFrame)
        let outputBufferSize = framesPerBuffer * Int(outputFormat.mBytesPerFrame)
        var outputBuffer = Data(count: outputBufferSize)
        
        // Setup input buffer
        var inputBuffer = inputData
        var inputBufferList = AudioBufferList()
        inputBufferList.mNumberBuffers = 1
        inputBufferList.mBuffers.mNumberChannels = inputFormat.mChannelsPerFrame
        inputBufferList.mBuffers.mDataByteSize = UInt32(inputData.count)
        inputBufferList.mBuffers.mData = UnsafeMutableRawPointer(mutating: (inputBuffer as NSData).bytes)
        
        // Setup output buffer
        var outputBufferList = AudioBufferList()
        outputBufferList.mNumberBuffers = 1
        outputBufferList.mBuffers.mNumberChannels = outputFormat.mChannelsPerFrame
        outputBufferList.mBuffers.mDataByteSize = UInt32(outputBufferSize)
        outputBufferList.mBuffers.mData = UnsafeMutableRawPointer(mutating: (outputBuffer as NSData).bytes)

        var outputFrames = UInt32(framesPerBuffer)
        let conversionStatus = AudioConverterFillComplexBuffer(
            converter!,
            { (inAudioConverter, ioNumberDataPackets, ioData, outDataPacketDescription, inUserData) -> OSStatus in
                if let inputBufferList = UnsafeMutablePointer<AudioBufferList>(OpaquePointer(inUserData)) {
                    ioData.pointee = inputBufferList.pointee
                    return noErr
                }
                return -1
            },
            &inputBufferList,
            &outputFrames,
            &outputBufferList,
            nil
        )
        
        guard conversionStatus == noErr else {
            print("Conversion failed with status: \(conversionStatus)")
            return nil
        }
        
        return outputBuffer
    }

    private func processPCMData(_ entry: AudioEntry, _ data: Data) {
        let bytesPerFrame = entry.audioStreamFormat.mBytesPerFrame
        let framesInData = UInt32(data.count) / UInt32(bytesPerFrame)
        
        rendererContext.lock.lock()
        
        let frameCount = rendererContext.bufferContext.frameUsedCount
        if frameCount == 0 {
            rendererContext.bufferContext.frameStartIndex = 0
        }
        
//        let availableFrames = rendererContext.bufferContext.totalFrameCount - frameCount
//        guard availableFrames >= framesInData else {
//            rendererContext.lock.unlock()
//            Logger.debug("Buffer full, cannot append more PCM data", category: .generic)
//            return
//        }
        
        // Debug: Print buffer state
        print("Buffer state before copy:")
        print("Start index:", rendererContext.bufferContext.frameStartIndex)
        print("Used count:", rendererContext.bufferContext.frameUsedCount)
        
        
        let destOffset = Int(rendererContext.bufferContext.frameStartIndex + frameCount) * Int(bytesPerFrame)
        let destPtr = rendererContext.audioBuffer.mData?.advanced(by: destOffset)
        
        print("DEST OFFSET \(destOffset) DEST PTR \(destPtr) BYTES COUNTL \(data.count)")
        data.withUnsafeBytes { rawBufferPointer in
            if let sourcePtr = rawBufferPointer.baseAddress {
                destPtr?.copyMemory(from: sourcePtr, byteCount: data.count)
            }
        }
        
//        let totalBytes = frameCount * bytesPerFrame
//        // Ensure we're copying complete frames
//        data.withUnsafeBytes { rawBufferPointer in
//            let int16Ptr = rawBufferPointer.bindMemory(to: Int16.self)
//            for i in 0..<min(10, int16Ptr.count) {
//                print("Sample \(i): \(int16Ptr[i])")
//            }
//        }
        
        print("Buffer state after copy:")
        print("Used count:", rendererContext.bufferContext.frameUsedCount)
        
        updatePCMProperties(entry: entry, newDataSize: data.count)
        
        rendererContext.lock.unlock()
        
        // Use the same fillUsedFrames function for consistency
        fillUsedFrames(framesCount: framesInData)

        // Debug: Print final state

        if rendererContext.waiting.value {
            rendererContext.packetsSemaphore.signal()
        }

    }

    @inline(__always)
    private func fillUsedFrames(framesCount: UInt32) {
        rendererContext.lock.lock()
        rendererContext.bufferContext.frameUsedCount += framesCount
        rendererContext.lock.unlock()

        playerContext.audioReadingEntry?.lock.lock()
        playerContext.audioReadingEntry?.framesState.queued += Int(framesCount)
        playerContext.audioReadingEntry?.lock.unlock()
    }

    func debugPrintWAVHeader(data: Data) {
        guard data.count >= 44 else {
            print("Data too small to be a WAV header")
            return
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
                break
            }
            
            offset += 8 + Int(chunkSize)
            if offset % 2 == 1 { offset += 1 }  // Padding byte if chunk size is odd
        }
    }
}

extension AudioPlayer: AudioStreamSourceDelegate {
    public func dataAvailable(source: CoreAudioStreamSource, data: Data) {
        guard let readingEntry = playerContext.audioReadingEntry, readingEntry.has(same: source) else {
            return
        }

        if source.audioFileHint == kAudioFileWAVEType {
            if let floatPCMData = convertToFloat32(entry: readingEntry, data) {
                processPCMData(readingEntry, floatPCMData)
            }
            return
        }

        if !fileStreamProcessor.isFileStreamOpen {
            let openFileStreamStatus = fileStreamProcessor.openFileStream(with: source.audioFileHint)
            guard openFileStreamStatus == noErr else {
                let streamError = AudioFileStreamError(status: openFileStreamStatus)
                raiseUnexpected(error: .audioSystemError(.fileStreamError(streamError)))
                return
            }
        }

        if fileStreamProcessor.isFileStreamOpen {
            let streamBytesStatus = fileStreamProcessor.parseFileStreamBytes(data: data)
            guard streamBytesStatus == noErr else {
                if let playingEntry = playerContext.audioPlayingEntry, playingEntry.has(same: source) {
                    let streamBytesError = AudioFileStreamError(status: streamBytesStatus)
                    raiseUnexpected(error: .streamParseBytesFailure(streamBytesError))
                }
                return
            }

            if playerContext.audioReadingEntry == nil {
                source.close()
            }
        }
    }
    
    public func errorOccurred(source: CoreAudioStreamSource, error: Error) {
        guard let entry = playerContext.audioReadingEntry, entry.has(same: source) else { return }
        raiseUnexpected(error: .networkError(.failure(error)))
    }

    public func endOfFileOccurred(source: CoreAudioStreamSource) {
        let hasSameSource = playerContext.audioReadingEntry?.has(same: source) ?? false
        guard playerContext.audioReadingEntry == nil || hasSameSource else {
            source.delegate = nil
            source.close()
            return
        }
        let queuedItemId = playerContext.audioReadingEntry?.id
        asyncOnMain { [weak self] in
            guard let self = self else { return }
            guard let itemId = queuedItemId else { return }
            self.delegate?.audioPlayerDidFinishBuffering(player: self, with: itemId)
        }

        guard let readingEntry = playerContext.audioReadingEntry else {
            source.delegate = nil
            source.close()
            return
        }

        readingEntry.lock.lock()
        readingEntry.framesState.lastFrameQueued = readingEntry.framesState.queued
        readingEntry.lock.unlock()

        readingEntry.delegate = nil
        readingEntry.close()

        playerContext.entriesLock.lock()
        playerContext.audioReadingEntry = nil
        playerContext.entriesLock.unlock()

        sourceQueue.async { [weak self] in
            guard let self = self else { return }
            self.processSource()
        }
    }

    public func metadataReceived(data: [String: String]) {
        asyncOnMain { [weak self] in
            guard let self = self else { return }
            self.delegate?.audioPlayerDidReadMetadata(player: self, metadata: data)
        }
    }
}
