import Foundation
import SwiftUI
import MWDATCore

// MARK: - CaptureManager

/// Manages capture provider selection, lifecycle, and glasses registration state.
/// Auto-selects glasses when connected, falls back to phone camera.
/// On Simulator (no camera/mic), starts with empty streams to avoid crashes.
@MainActor
final class CaptureManager: ObservableObject {

    // MARK: - Properties

    @Published private(set) var activeProvider: (any CaptureProvider)?
    @Published private(set) var activeSource: CaptureSource = .phone

    // Glasses registration state (for UI)
    @Published private(set) var registrationState: RegistrationState
    @Published private(set) var connectedDevices: [DeviceIdentifier] = []
    @Published var glassesError: String?

    private var glassesProvider: GlassesCaptureProvider
    private var phoneProvider: PhoneCaptureProvider
    private let wearables: WearablesInterface

    private var registrationTask: Task<Void, Never>?
    private var deviceStreamTask: Task<Void, Never>?
    private var previousRegistrationState: RegistrationState?
    private var isStartingRegistration = false

    // MARK: - Computed

    var isGlassesRegistered: Bool {
        registrationState == .registered
    }

    var isRegistering: Bool {
        registrationState == .registering
    }

    var firstDeviceName: String? {
        guard let deviceId = connectedDevices.first,
              let device = wearables.deviceForIdentifier(deviceId) else { return nil }
        return device.nameOrId()
    }

    // MARK: - Init

    init(wearables: WearablesInterface = Wearables.shared) {
        self.wearables = wearables
        self.registrationState = wearables.registrationState
        self.glassesProvider = GlassesCaptureProvider(wearables: wearables)
        self.phoneProvider = PhoneCaptureProvider()

        NSLog("[CaptureManager] Initialized — registration: \(wearables.registrationState), phoneAvailable: \(phoneProvider.isAvailable)")

        // Monitor registration state
        registrationTask = Task { @MainActor [weak self, wearables] in
            for await state in wearables.registrationStateStream() {
                guard let self else { break }

                if self.previousRegistrationState == .registering,
                   state == .available {
                    self.isStartingRegistration = false
                    if self.glassesError == nil {
                        self.glassesError = """
                        Registration didn't finish. In Meta AI, approve the connection and return to NoteV. \
                        Confirm your release channel is published, your account is added as a test user, \
                        and glasses firmware is up to date (v125+). For Oakley Meta Vanguard, install the \
                        DAT app on the glasses from Meta AI → Settings if prompted.
                        """
                    }
                    NSLog("[CaptureManager] Registration rolled back to available without completing")
                }

                if state == .registered {
                    self.isStartingRegistration = false
                    self.glassesError = nil
                }

                self.previousRegistrationState = state
                self.registrationState = state
                NSLog("[CaptureManager] Registration state: \(state)")
            }
        }

        // Monitor connected devices
        deviceStreamTask = Task { @MainActor [weak self, wearables] in
            for await devices in wearables.devicesStream() {
                guard let self else { break }
                self.connectedDevices = devices
                NSLog("[CaptureManager] Connected devices: \(devices.count)")
            }
        }
    }

    deinit {
        registrationTask?.cancel()
        deviceStreamTask?.cancel()
    }

    // MARK: - Glasses Connection

    func connectGlasses() {
        guard registrationState != .registering, !isStartingRegistration else { return }
        isStartingRegistration = true
        glassesError = nil
        Task { @MainActor in
            defer { isStartingRegistration = false }
            do {
                try await wearables.startRegistration()
                NSLog("[CaptureManager] Registration started")
            } catch let error as RegistrationError {
                NSLog("[CaptureManager] Registration error (RegistrationError): \(error.description)")
                glassesError = error.description
            } catch {
                NSLog("[CaptureManager] Registration error: \(error)")
                glassesError = error.localizedDescription
            }
        }
    }

    func disconnectGlasses() {
        Task { @MainActor in
            do {
                try await wearables.startUnregistration()
                NSLog("[CaptureManager] Unregistration started")
            } catch let error as UnregistrationError {
                NSLog("[CaptureManager] Unregistration error: \(error.description)")
                glassesError = error.description
            } catch {
                NSLog("[CaptureManager] Unregistration error: \(error)")
                glassesError = error.localizedDescription
            }
        }
    }

    func dismissGlassesError() {
        glassesError = nil
    }

    /// Recreates capture providers with fresh AsyncStreams while keeping registration/device state.
    func resetProvidersForNewSession() {
        glassesProvider = GlassesCaptureProvider(wearables: wearables)
        phoneProvider = PhoneCaptureProvider()
        activeProvider = nil
        activeSource = .phone
    }

    /// Ensures Meta AI camera permission is granted before starting a glasses session.
    func ensureGlassesCameraPermission() async throws {
        guard !connectedDevices.isEmpty else {
            throw GlassesCaptureProvider.makeError(
                code: -11,
                message: "Glasses aren't connected. Open Meta AI, confirm your glasses are paired, then try again."
            )
        }
        glassesProvider.notePairedDeviceCount(atLeast: connectedDevices.count)
        try await glassesProvider.ensureCameraPermission()
    }

    // MARK: - Provider Selection

    /// Select capture provider based on user's explicit choice.
    /// When `preferredSource` is `.glasses`, uses glasses provider directly
    /// (bypasses async isAvailable check — the UI already confirmed glasses are connected).
    func selectProvider(preferredSource: CaptureSource = .phone) -> any CaptureProvider {
        if preferredSource == .glasses {
            activeSource = .glasses
            activeProvider = glassesProvider
            NSLog("[CaptureManager] Selected glasses capture provider (user choice)")
            return glassesProvider
        } else if phoneProvider.isAvailable {
            activeSource = .phone
            activeProvider = phoneProvider
            NSLog("[CaptureManager] Selected phone capture provider")
            return phoneProvider
        } else {
            // Simulator or no hardware — use phone provider anyway (it will produce empty streams)
            activeSource = .phone
            activeProvider = phoneProvider
            NSLog("[CaptureManager] WARNING: No capture hardware available (Simulator?) — using phone provider stub")
            return phoneProvider
        }
    }

    // MARK: - Lifecycle

    /// Start capture with the user-selected provider.
    func startCapture(preferredSource: CaptureSource = .phone) async throws {
        let provider = selectProvider(preferredSource: preferredSource)
        if let glasses = provider as? GlassesCaptureProvider {
            glasses.notePairedDeviceCount(atLeast: connectedDevices.count)
        }
        do {
            try await provider.startCapture()
            NSLog("[CaptureManager] Capture started via \(activeSource.rawValue)")
        } catch {
            NSLog("[CaptureManager] WARNING: Capture start failed: \(error.localizedDescription) — continuing with empty streams")
            #if !targetEnvironment(simulator)
            throw error
            #endif
        }
    }

    /// Stop capture.
    func stopCapture() async {
        await activeProvider?.stopCapture()
        NSLog("[CaptureManager] Capture stopped")
    }

    /// Drain in-flight capture buffers before finalizing MP4.
    func flushPendingSamples() async {
        await activeProvider?.flushPendingSamples()
    }
}
