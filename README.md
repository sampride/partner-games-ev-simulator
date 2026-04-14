readme_content = """# ⚡ Partner Games Simulator: EV Charging Hub

## Overview
The Partner Games Simulator is a high-performance, asynchronous Python 3.13 engine designed to generate realistic, multivariate IoT telemetry for Machine Learning anomaly detection hackathons (specifically targeting platforms like TwinThread). 

Instead of simple binary failures, this simulator models the complex physical interactions of a multi-bay DC Fast Charging Hub. It generates baseline operational data and continuous mathematical divergences (anomalies) that eventually cascade into hardware protection trips (faults).

## Core Architecture
The simulator is built on a decoupled, high-frequency async architecture designed to run efficiently on edge hardware (like a Raspberry Pi 5).

1. **The Async Engine:** Uses an `asyncio` event loop ticking precisely at 20Hz (50ms).
2. **Producer/Consumer Queue:** The physics calculations (Producer) are decoupled from file/network I/O (Consumer) via a fast memory queue. This ensures network latency never degrades the physical modeling.
3. **Hierarchical Assets:**
    * **`ChargingSite` (Site Controller):** Manages ambient weather, dynamic grid voltage sags based on total load, and simulates vehicle arrivals using a diurnal Poisson distribution with queue balking.
    * **`EVCharger`:** A state-machine-driven model (Idle -> Handshaking -> Charging -> Throttled -> Faulted). Calculates $I^2R$ electrical heat generation, Newton's Law of Cooling (active air and liquid cooling), and vehicle battery State of Charge (SOC) curves.
4. **Resilient Writers:**
    * **CSV Writer:** Handles massive historical backfills, automatically organizing data into date-based folders.
    * **MQTT Writer:** Uses `paho-mqtt` (v2 API) to publish grouped JSON arrays to hierarchical topics. Features an internal offline buffer (`collections.deque`) and exponential backoff to handle network drops and Mosquitto container reboot sequencing.

## Telemetry & Polling Physics
Data is output in a "narrow" EAV (Entity-Attribute-Value) format. To maintain physical realism and optimize bandwidth, sensors poll at staggered frequencies:

| Sensor | Rate | Rationale |
| :--- | :--- | :--- |
| `Grid_Voltage_AC`, `Output_Voltage_DC`, `Output_Current_DC`, `Requested_Current_DC` | **20Hz** | Captures millisecond electrical transients and noise. |
| `Cooling_Fan_RPM`, `Coolant_Flow_LPM`, `Coolant_Pressure_kPa` | **2Hz** | Mechanical systems have physical inertia. |
| `Ambient_Temp`, `Cabinet_Temp`, `Cable_Connector_Temp` | **0.5Hz** | Thermal masses change very slowly over time. |
| `EV_State_of_Charge`, `Charger_State`, `Session_Duration`, `Error_Code` | **1Hz** | Logical state tracking. |

## The Data Narrative: Anomalies vs. Faults
The core challenge of the dataset is detecting the *Anomaly* before it becomes a *Fault*.

* **The Anomaly (The Target):** A mathematical divergence in the physics model (e.g., a fan slowly seizing). The charger continues to operate, but variables lose their normal correlations. **This is intentionally not labeled in the data stream.**
* **The Fault (The Result):** If an anomaly is ignored, the physical protection relay trips (e.g., Cabinet Temp hits 85°C). The charger enters State `4`, drops current to `0A`, and emits `Error_Code: 1`. 

### Supported Anomalies
1. **`FAN_FAILURE` (Mechanical):** Fan RPM drops to 0. Active air cooling is removed. Internal temperatures rise exponentially, eventually triggering thermal throttling (current drop to 50A) and ultimately an OverTemp fault.
2. **`CONNECTOR_ARCING` (Electrical/Thermal):** Induces heavy statistical variance on the 20Hz electrical current output and drives aggressive localized heat buildup in the cable, breaking the normal $I^2R$ correlation.
3. **`BMS_CHATTER` (Behavioral):** The vehicle's Battery Management System requests rapidly oscillating current limits, causing systemic grid sags across the site.

## Configuration (`default_sim.yaml`)
The entire simulation is driven by a single YAML file, utilizing YAML Anchors (`&`) to keep definitions DRY.

### Anomaly Scheduling
You can configure chargers to be perfectly reliable, fail on a strict schedule (ideal for live presentations), or experience randomized chaos.