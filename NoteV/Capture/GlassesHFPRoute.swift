import AVFoundation
import Foundation

// MARK: - GlassesHFPRoute

/// Helpers for routing glasses microphone audio over Bluetooth HFP per Meta DAT docs.
/// https://wearables.developer.meta.com/docs/develop/dat/microphones-and-speakers/
enum GlassesHFPRoute {

    private static let glassesNameHints = ["meta", "oakley", "ray-ban", "rayban", "glasses"]

    private static let nonGlassesNameHints = ["airpods", "airpod", "beats", "headphone", "headset", "car"]

    /// Whether the active input route includes Bluetooth HFP (glasses mic).
    static func isActive(on session: AVAudioSession = .sharedInstance()) -> Bool {
        session.currentRoute.inputs.contains { $0.portType == .bluetoothHFP }
    }

    /// Active HFP input port name, if any.
    static func activeInputName(on session: AVAudioSession = .sharedInstance()) -> String? {
        session.currentRoute.inputs.first { $0.portType == .bluetoothHFP }?.portName
    }

    /// True when the active HFP route looks like glasses (Meta / Oakley / Ray-Ban).
    static func isLikelyGlassesInput(on session: AVAudioSession = .sharedInstance()) -> Bool {
        guard let name = activeInputName(on: session)?.lowercased() else { return false }
        if nonGlassesNameHints.contains(where: { name.contains($0) }) { return false }
        return glassesNameHints.contains(where: { name.contains($0) }) || name.contains("02tt")
    }

    /// Prefer the HFP input that looks like glasses; fall back to any HFP port.
    static func configurePreferredInput(on session: AVAudioSession = .sharedInstance()) throws {
        let hfpInputs = session.availableInputs?.filter { $0.portType == .bluetoothHFP } ?? []
        guard !hfpInputs.isEmpty else { return }

        let glassesInput = hfpInputs.first { input in
            let name = input.portName.lowercased()
            if nonGlassesNameHints.contains(where: { name.contains($0) }) { return false }
            return glassesNameHints.contains(where: { name.contains($0) }) || name.contains("02tt")
        }

        let selected = glassesInput ?? hfpInputs[0]
        try session.setPreferredInput(selected)
        NSLog("[GlassesHFPRoute] Preferred input set to HFP: \(selected.portName)")

        if glassesInput == nil, nonGlassesNameHints.contains(where: { selected.portName.lowercased().contains($0) }) {
            NSLog("[GlassesHFPRoute] WARNING: HFP route is '\(selected.portName)' — expected Meta/Oakley glasses mic, not headphones")
        }
    }

    /// Meta recommends ~2s for the HFP route to settle after starting AVAudioEngine.
    static func waitForActive(timeoutSeconds: TimeInterval = 2.5) async -> Bool {
        let deadline = Date().addingTimeInterval(timeoutSeconds)
        while Date() < deadline {
            if isActive() { return true }
            try? await Task.sleep(nanoseconds: 100_000_000)
        }
        return isActive()
    }

    static var missingRouteError: NSError {
        NSError(
            domain: "GlassesCaptureProvider",
            code: -11,
            userInfo: [NSLocalizedDescriptionKey:
                "Glasses microphone not connected. Wear your glasses, confirm they appear in Meta AI, then try again. Audio must route through Bluetooth headset (HFP), not the iPhone mic."]
        )
    }

    static var wrongHFPDeviceError: NSError {
        NSError(
            domain: "GlassesCaptureProvider",
            code: -12,
            userInfo: [NSLocalizedDescriptionKey:
                "Audio is routing to \(activeInputName() ?? "another Bluetooth device") instead of your glasses. Disconnect other Bluetooth headphones and try again."]
        )
    }
}
