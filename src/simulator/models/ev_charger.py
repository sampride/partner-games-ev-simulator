import math
import random
from datetime import datetime
from simulator.models.base import Asset, SensorConfig


class EVCharger(Asset):
    def __init__(self, name: str) -> None:
        super().__init__(name)

        self.sensors = [
            # Electrical (20Hz)
            SensorConfig("Grid_Voltage_AC", 0.05, 0.0),
            SensorConfig("Output_Voltage_DC", 0.05, 0.0),
            SensorConfig("Output_Current_DC", 0.05, 0.0),
            SensorConfig("Requested_Current_DC", 0.05, 0.0),
            # Mechanical & Cooling (2 Hz)
            SensorConfig("Cooling_Fan_RPM", 0.5, 0.0),
            SensorConfig("Coolant_Flow_LPM", 0.5, 0.0),
            SensorConfig("Coolant_Pressure_kPa", 0.5, 0.0),
            # Thermal (0.5Hz)
            SensorConfig("Ambient_Temp", 2.0, 0.0),
            SensorConfig("Cabinet_Temp", 2.0, 0.0),
            SensorConfig("Cable_Connector_Temp", 2.0, 0.0),
            # State & Metadata (1Hz)
            SensorConfig("EV_State_of_Charge", 1.0, 0.0),
            SensorConfig("Charger_State", 1.0, 0.0),
            SensorConfig("Session_Duration", 1.0, 0.0),
            SensorConfig("Error_Code", 1.0, 0.0),  # <-- NEW
        ]

        self.state = {
            "charger_state": 0,  # 0=Idle, 1=Handshaking, 2=Charging, 3=Throttled, 4=Fault
            "error_code": 0,  # 0=None, 1=Thermal_Trip, 2=Grid_Fault (for future use)
            "session_duration_sec": 0.0,
            # Vehicle Variables
            "ev_battery_capacity_kwh": 0.0,
            "ev_soc_percent": 0.0,
            "ev_base_voltage": 400.0,
            # Electrical
            "requested_current_a": 0.0,
            "output_current_a": 0.0,
            "current_power_kw": 0.0,
            # Thermal & Mechanical
            "cabinet_temp_c": 25.0,
            "cable_temp_c": 25.0,
            "cooling_fan_rpm": 0.0,
            "coolant_flow_lpm": 0.0,
            "simulation_uptime_sec": 0.0,
            "active_anomaly": "NONE",
            "anomaly_end_time_sec": 0.0,
        }

        # Format: {"type": "FAN_FAILURE", "start_sec": 600, "duration_sec": 1800}
        self.scheduled_anomalies: list[dict] = []

        self.random_anomaly_config = {
            "enabled": False,
            "chance_per_hour": 0.0,
            "types": ["FAN_FAILURE", "CONNECTOR_ARCING", "BMS_CHATTER"],
            "min_duration_sec": 1800,
            "max_duration_sec": 7200,
        }

    def start_session(self, duration_sec: float) -> None:
        if self.state["charger_state"] == 4:
            return  # Ignore connection attempts if faulted

        self.state["charger_state"] = 1
        self.state["session_duration_sec"] = 0.0
        self.state["ev_battery_capacity_kwh"] = random.uniform(60.0, 100.0)
        self.state["ev_soc_percent"] = random.uniform(10.0, 40.0)
        self.state["ev_base_voltage"] = random.choice([400.0, 800.0])
        self.state["requested_current_a"] = (
            200.0 if self.state["ev_base_voltage"] == 400.0 else 350.0
        )

    def update_internal_state(
        self, delta_sec: float, current_time: datetime, global_state: dict
    ) -> None:
        ambient_temp = global_state.get("ambient_temp_c", 25.0)

        # 0. UPTIME & ANOMALY INJECTION
        self.state["simulation_uptime_sec"] += delta_sec

        # A. Check if a running anomaly has expired
        if self.state["active_anomaly"] != "NONE":
            if self.state["simulation_uptime_sec"] >= self.state["anomaly_end_time_sec"]:
                self.state["active_anomaly"] = "NONE"

        # B. Check Scheduled Anomalies (These override random ones)
        for anomaly in self.scheduled_anomalies:
            start = anomaly.get("start_sec", 0)
            end = start + anomaly.get("duration_sec", 0)
            if start <= self.state["simulation_uptime_sec"] < end:
                self.state["active_anomaly"] = anomaly.get("type", "NONE")
                self.state["anomaly_end_time_sec"] = end
                break

                # C. Roll the dice for a new Random Anomaly
        if self.state["active_anomaly"] == "NONE" and self.random_anomaly_config.get(
            "enabled", False
        ):
            # Convert chance per hour to chance per tick
            chance_per_hr = self.random_anomaly_config.get("chance_per_hour", 0.01)
            prob_per_tick = (chance_per_hr / 3600.0) * delta_sec

            if random.random() < prob_per_tick:
                types = self.random_anomaly_config.get("types", ["FAN_FAILURE"])
                self.state["active_anomaly"] = random.choice(types)

                min_dur = self.random_anomaly_config.get("min_duration_sec", 1800.0)
                max_dur = self.random_anomaly_config.get("max_duration_sec", 7200.0)
                duration = random.uniform(min_dur, max_dur)

                self.state["anomaly_end_time_sec"] = self.state["simulation_uptime_sec"] + duration

        # 1. HARD FAULT LOGIC (The physical protection relay)
        if self.state["cabinet_temp_c"] >= 85.0 and self.state["charger_state"] != 4:
            # Trip the breaker immediately
            self.state["charger_state"] = 4
            self.state["error_code"] = 1  # 1 = OverTemp
            self.state["requested_current_a"] = 0.0
            self.state["output_current_a"] = 0.0  # Instant drop, no ramp down
            self.state["current_power_kw"] = 0.0

        # 2. FAULT RECOVERY LOGIC (Auto-Clear)
        if self.state["charger_state"] == 4:
            # Wait for passive/active cooling to bring it back to a safe baseline
            if self.state["cabinet_temp_c"] <= 45.0:
                self.state["charger_state"] = 0
                self.state["error_code"] = 0
                # Session is wiped out, customer drove away angry
                self.state["ev_soc_percent"] = 0.0

        # 3. State Machine & Time
        if self.state["charger_state"] in [1, 2, 3]:
            self.state["session_duration_sec"] += delta_sec
            if self.state["charger_state"] == 1 and self.state["session_duration_sec"] > 5.0:
                self.state["charger_state"] = 2
            if self.state["ev_soc_percent"] >= 95.0:
                self.state["charger_state"] = 0
                self.state["requested_current_a"] = 0.0

        # 4. Thermal Throttling Logic (Preventative)
        if self.state["charger_state"] in [2, 3]:
            if self.state["cabinet_temp_c"] > 65.0:
                self.state["charger_state"] = 3
                self.state["requested_current_a"] = 50.0
            elif self.state["cabinet_temp_c"] < 55.0 and self.state["charger_state"] == 3:
                self.state["charger_state"] = 2
                self.state["requested_current_a"] = (
                    200.0 if self.state["ev_base_voltage"] == 400.0 else 350.0
                )

        # ANOMALY INJECTION: BMS Chattering (Behavioral Fault)
        if self.state["active_anomaly"] == "BMS_CHATTER" and self.state[
            "charger_state"
        ] in [2, 3]:
            # Rapidly oscillate the requested current between 10% and 100%
            chatter_factor = 0.55 + 0.45 * math.sin(current_time.timestamp() * 4.0)
            self.state["requested_current_a"] *= chatter_factor

        # 5. Electrical Math
        if self.state["charger_state"] == 0:
            self.state["requested_current_a"] = 0.0

        # Only ramp current if we aren't faulted (fault drops instantly to 0)
        if self.state["charger_state"] != 4:
            curr_diff = self.state["requested_current_a"] - self.state["output_current_a"]
            self.state["output_current_a"] += curr_diff * 2.0 * delta_sec

        current = self.state["output_current_a"]

        if current > 1.0 and self.state["charger_state"] != 4:
            dc_voltage = self.state["ev_base_voltage"] + (
                self.state["ev_soc_percent"] / 100.0 * 50.0
            )
            self.state["current_power_kw"] = (current * dc_voltage) / 1000.0
            kwh_added = (self.state["current_power_kw"] * delta_sec) / 3600.0
            self.state["ev_soc_percent"] += (
                kwh_added / self.state["ev_battery_capacity_kwh"]
            ) * 100.0
        else:
            self.state["current_power_kw"] = 0.0

        # ANOMALY INJECTION: Connector Arcing (Heat Divergence)
        if self.state["active_anomaly"] == "CONNECTOR_ARCING" and self.state[
            "charger_state"
        ] in [2, 3]:
            # Arcing generates massive localized heat at the pin, independent of normal I^2R losses
            self.state["cable_temp_c"] += 2.0 * delta_sec

        # 6. Thermal & Mechanical Math
        heat_generated = (current / 200.0) ** 2 * 6.0

        # Fan runs hard if hot, even if faulted, to cool the system down
        target_rpm = max(0.0, min(4500.0, (self.state["cabinet_temp_c"] - 30.0) * 300.0))

        # ANOMALY INJECTION: Fan Seizure (Mechanical Fault)
        if self.state["active_anomaly"] == "FAN_FAILURE":
            target_rpm = 0.0


        self.state["cooling_fan_rpm"] += (
            (target_rpm - self.state["cooling_fan_rpm"]) * 1.0 * delta_sec
        )
        active_air_cooling = (self.state["cooling_fan_rpm"] / 4500.0) * 8.0

        self.state["cabinet_temp_c"] += (
            heat_generated
            - active_air_cooling
            - ((self.state["cabinet_temp_c"] - ambient_temp) * 0.05)
        ) * delta_sec

        target_lpm = max(0.0, min(15.0, (self.state["cable_temp_c"] - 30.0) * 1.5))
        self.state["coolant_flow_lpm"] += (
            (target_lpm - self.state["coolant_flow_lpm"]) * 0.5 * delta_sec
        )
        active_liquid_cooling = (self.state["coolant_flow_lpm"] / 15.0) * 4.0

        cable_heat = (current / 200.0) ** 2 * 3.0
        self.state["cable_temp_c"] += (
            cable_heat - active_liquid_cooling - ((self.state["cable_temp_c"] - ambient_temp) * 0.1)
        ) * delta_sec

    def read_sensor(self, sensor_name: str, global_state: dict) -> float:
        is_active = self.state["charger_state"] in [1, 2, 3]

        # Convert the string anomaly into a numeric label for ML (0=Normal, 1=Fan, 2=Arc, 3=Chatter)
        # anomaly_map = {"NONE": 0.0, "FAN_FAILURE": 1.0, "CONNECTOR_ARCING": 2.0, "BMS_CHATTER": 3.0}

        match sensor_name:
            case "Grid_Voltage_AC":
                return round(
                    global_state.get("current_grid_voltage", 400.0) + random.gauss(0, 1.0), 2
                )
            case "Output_Voltage_DC":
                if not is_active:
                    return 0.0
                base_v = self.state["ev_base_voltage"] + (
                    self.state["ev_soc_percent"] / 100.0 * 50.0
                )
                return round(base_v + random.gauss(0, 0.5), 2)
            case "Output_Current_DC":
                if self.state["output_current_a"] < 1.0:
                    return 0.0

                # ANOMALY INJECTION: Arcing causes massive variance on the high-speed electrical read
                noise_multiplier = 5.0 if self.state["active_anomaly"] == "CONNECTOR_ARCING" else 0.5
                return round(self.state["output_current_a"] + random.gauss(0, noise_multiplier), 2)

            case "Requested_Current_DC":
                return round(self.state["requested_current_a"], 2)
            case "Cooling_Fan_RPM":
                return (
                    round(
                        self.state["cooling_fan_rpm"]
                        + random.gauss(0, self.state["cooling_fan_rpm"] * 0.02),
                        0,
                    )
                    if self.state["cooling_fan_rpm"] > 10
                    else 0.0
                )
            case "Coolant_Flow_LPM":
                return (
                    round(
                        self.state["coolant_flow_lpm"]
                        + random.gauss(0, self.state["coolant_flow_lpm"] * 0.05),
                        2,
                    )
                    if self.state["coolant_flow_lpm"] > 0.5
                    else 0.0
                )
            case "Coolant_Pressure_kPa":
                return (
                    round(
                        150.0 + (self.state["coolant_flow_lpm"] ** 2 * 0.8) + random.gauss(0, 2.0),
                        1,
                    )
                    if self.state["coolant_flow_lpm"] >= 0.5
                    else 150.0
                )
            case "Ambient_Temp":
                return round(global_state.get("ambient_temp_c", 25.0) + random.gauss(0, 0.2), 2)
            case "Cabinet_Temp":
                return round(self.state["cabinet_temp_c"] + random.gauss(0, 0.1), 2)
            case "Cable_Connector_Temp":
                return round(self.state["cable_temp_c"] + random.gauss(0, 0.1), 2)
            case "EV_State_of_Charge":
                return round(self.state["ev_soc_percent"], 1) if is_active else 0.0
            case "Charger_State":
                return float(self.state["charger_state"])
            case "Session_Duration":
                return round(self.state["session_duration_sec"], 0) if is_active else 0.0
            case "Error_Code": return float(self.state["error_code"]) # <-- NEW
            case _: return 0.0