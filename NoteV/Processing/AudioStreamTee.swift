import Foundation

// MARK: - AudioStreamTee

/// Fans a single capture audio stream to multiple consumers (STT + MP4 mux).
enum AudioStreamTee {

    static func tee(_ source: AsyncStream<AudioChunk>) -> (stt: AsyncStream<AudioChunk>, mux: AsyncStream<AudioChunk>) {
        let (sttStream, sttContinuation) = AsyncStream<AudioChunk>.makeStream()
        let (muxStream, muxContinuation) = AsyncStream<AudioChunk>.makeStream()

        Task.detached(priority: .userInitiated) {
            for await chunk in source {
                sttContinuation.yield(chunk)
                muxContinuation.yield(chunk)
            }
            sttContinuation.finish()
            muxContinuation.finish()
        }

        return (sttStream, muxStream)
    }
}
