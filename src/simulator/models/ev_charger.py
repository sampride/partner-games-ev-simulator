import logging
import math
import random
from datetime import datetime
from typing import Any

from simulator.models.base import Asset, SensorConfig

logger = logging.getLogger("simulator.models.ev_charger")


class EVCharger(Asset):
    STATE_IDLE = 0
    STATE_HANDSHAKING = 1
    STATE_CHARGING = 2
    STATE_THROTTLED = 3
    STATE_FAULT = 4

    ERROR_NONE = 0
    ERROR_OVERTEMP = 1

    def __init__(self, name: str) -> None:
        super().__init__(name)

        self.sensors = [
            # Electrical
            SensorConfig("Grid_Voltage_AC", 0.05, 0.0),
            SensorConfig("Grid_Current_AC", 0.2, 0.0),
            SensorConfig("Input_Power_kW", 0.2, 0.0),
            SensorConfig("DC_Bus_Voltage", 0.1, 0.0),
            SensorConfig("Output_Voltage_DC", 0.05, 0.0),
            SensorConfig("Output_Current_DC", 0.05, 0.0),
            SensorConfig("Requested_Current_DC", 0.05, 0.0),
            # Thermal and cooling
            SensorConfig("Cooling_Fan_RPM", 0.5, 0.0),
            SensorConfig("Pump_Speed_RPM", 0.5, 0.0),
            SensorConfig("Coolant_Flow_LPM", 0.5, 0.0),
            SensorConfig("Coolant_Pressure_kPa", 0.5, 0.0),
            SensorConfig("Coolant_Inlet_Temp", 1.0, 0.0),
            SensorConfig("Coolant_Outlet_Temp", 1.0, 0.0),
            SensorConfig("Ambient_Temp", 2.0, 0.0),
            SensorConfig("Cabinet_Temp", 1.0, 0.0),
            SensorConfig("Power_Module_Temp", 1.0, 0.0),
            SensorConfig("Cable_Connector_Temp", 1.0, 0.0),
            # Health and state
            SensorConfig("Connector_Resistance_mOhm", 1.0, 0.0),
            SensorConfig("Derate_Level_Percent", 1.0, 0.0),
            SensorConfig("EV_State_of_Charge", 1.0, 0.0),
            SensorConfig("Charger_State", 1.0, 0.0),
            SensorConfig("Session_Duration", 1.0, 0.0),
            SensorConfig("Warning_Code", 1.0, 0.0),
            SensorConfig("Error_Code", 1.0, 0.0),
        ]

        self._refresh_next_sensor_due()

        self.state: dict[str, Any] = {
            "charger_state": self.STATE_IDLE,
            "error_code": self.ERROR_NONE,
            "warning_code": 0,
            "session_duration_sec": 0.0,
            # vehicle and session
            "target_session_duration_sec": 1800.0,
            "ev_battery_capacity_kwh": 75.0,
            "ev_soc_percent": 0.0,
            "ev_base_voltage": 400.0,
            "base_requested_current_a": 0.0,
            "requested_current_a": 0.0,
            "output_current_a": 0.0,
            "output_voltage_v": 0.0,
            "current_power_kw": 0.0,
            "input_power_kw": 0.0,
            "grid_current_a": 0.0,
            "dc_bus_voltage_v": 720.0,
            # thermal and mechanical
            "cabinet_temp_c": 25.0,
            "power_module_temp_c": 25.0,
            "cable_temp_c": 25.0,
            "coolant_inlet_temp_c": 25.0,
            "coolant_outlet_temp_c": 25.0,
            "cooling_fan_rpm": 0.0,
            "pump_speed_rpm": 0.0,
            "coolant_flow_lpm": 0.0,
            "coolant_pressure_kpa": 150.0,
            "connector_resistance_mohm": 0.55,
            "derate_level_pct": 0.0,
            # anomaly and health
            "simulation_uptime_sec": 0.0,
            "active_anomaly": "NONE",
            "anomaly_end_time_sec": 0.0,
            "anomaly_severity": 0.0,
            "fan_health": 1.0,
            "pump_health": 1.0,
            "comms_health": 1.0,
            "sensor_health": 1.0,
            "contactor_stability": 1.0,
        }

        self.scheduled_anomalies: list[dict[str, Any]] = []
        self.random_anomaly_config: dict[str, Any] = {
            "enabled": False,
            "chance_per_hour": 0.0,
            "types": [
                "FAN_FAILURE",
                "CONNECTOR_ARCING",
                "BMS_CHATTER",
                "PUMP_DEGRADATION",
                "CONTACTOR_CHATTER",
                "SENSOR_DRIFT",
            ],
            "min_duration_sec": 1800,
            "max_duration_sec": 7200,
        }

        self._last_logged_anomaly = "NONE"
        self._last_logged_state = self.STATE_IDLE

    def start_session(self, duration_sec: float) -> None:
        if self.state["charger_state"] == self.STATE_FAULT:
            return

        battery_capacity_kwh = random.uniform(55.0, 100.0)
        base_voltage = random.choice([400.0, 800.0])
        initial_soc = random.uniform(10.0, 45.0)
        max_current = 220.0 if base_voltage == 400.0 else 320.0

        self.state.update(
            {
                "charger_state": self.STATE_HANDSHAKING,
                "error_code": self.ERROR_NONE,
                "warning_code": 0,
                "session_duration_sec": 0.0,
                "target_session_duration_sec": duration_sec,
                "ev_battery_capacity_kwh": battery_capacity_kwh,
                "ev_soc_percent": initial_soc,
                "ev_base_voltage": float(base_voltage),
                "base_requested_current_a": max_current,
                "requested_current_a": 0.0,
            }
        )

    def _determine_active_anomaly(self, delta_sec: float) -> None:
        uptime = self.state["simulation_uptime_sec"]
        active = "NONE"
        end_time = 0.0

        for anomaly in self.scheduled_anomalies:
            start = float(anomaly.get("start_sec", 0.0))
            duration = float(anomaly.get("duration_sec", 0.0))
            if start <= uptime < start + duration:
                active = str(anomaly.get("type", "NONE"))
                end_time = start + duration
                break

        if active == "NONE" and self.random_anomaly_config.get("enabled", False):
            if self.state["active_anomaly"] != "NONE" and uptime < self.state["anomaly_end_time_sec"]:
                active = self.state["active_anomaly"]
                end_time = self.state["anomaly_end_time_sec"]
            else:
                chance_per_hr = float(self.random_anomaly_config.get("chance_per_hour", 0.0))
                prob_per_tick = (chance_per_hr / 3600.0) * delta_sec
                if random.random() < prob_per_tick:
                    anomaly_types = list(self.random_anomaly_config.get("types", ["FAN_FAILURE"]))
                    active = random.choice(anomaly_types)
                    duration = random.uniform(
                        float(self.random_anomaly_config.get("min_duration_sec", 1800.0)),
                        float(self.random_anomaly_config.get("max_duration_sec", 7200.0)),
                    )
                    end_time = uptime + duration

        previous = self.state["active_anomaly"]
        self.state["active_anomaly"] = active
        self.state["anomaly_end_time_sec"] = end_time

        if active == "NONE":
            self.state["anomaly_severity"] = max(0.0, self.state["anomaly_severity"] - delta_sec / 900.0)
        else:
            time_left = max(1.0, end_time - uptime)
            ramp_window = min(1800.0, max(300.0, time_left * 0.5))
            self.state["anomaly_severity"] = min(
                1.0, self.state["anomaly_severity"] + (delta_sec / ramp_window)
            )

        if previous != active:
            logger.info("%s anomaly %s -> %s", self.name, previous, active)

    def _apply_health_drift(self, delta_sec: float, current_time: datetime) -> None:
        severity = float(self.state["anomaly_severity"])
        anomaly = self.state["active_anomaly"]

        # recovery towards healthy baselines
        self.state["fan_health"] += (1.0 - self.state["fan_health"]) * min(1.0, delta_sec / 3600.0)
        self.state["pump_health"] += (1.0 - self.state["pump_health"]) * min(1.0, delta_sec / 3600.0)
        self.state["comms_health"] += (1.0 - self.state["comms_health"]) * min(1.0, delta_sec / 2400.0)
        self.state["sensor_health"] += (1.0 - self.state["sensor_health"]) * min(1.0, delta_sec / 2400.0)
        self.state["contactor_stability"] += (
            (1.0 - self.state["contactor_stability"]) * min(1.0, delta_sec / 1800.0)
        )
        self.state["connector_resistance_mohm"] += (
            (0.55 - self.state["connector_resistance_mohm"]) * min(1.0, delta_sec / 2400.0)
        )

        if anomaly == "FAN_FAILURE":
            self.state["fan_health"] = max(0.05, 1.0 - (severity * 1.15))
        elif anomaly == "PUMP_DEGRADATION":
            self.state["pump_health"] = max(0.08, 1.0 - (severity * 0.95))
        elif anomaly == "CONNECTOR_ARCING":
            flare = 0.6 + 0.4 * math.sin(current_time.timestamp() * 0.25)
            self.state["connector_resistance_mohm"] = min(
                8.0,
                0.55 + (severity * 2.5) + max(0.0, flare) * severity * 2.0,
            )
        elif anomaly == "BMS_CHATTER":
            self.state["comms_health"] = max(0.2, 1.0 - (severity * 0.85))
        elif anomaly == "CONTACTOR_CHATTER":
            self.state["contactor_stability"] = max(0.1, 1.0 - (severity * 1.05))
        elif anomaly == "SENSOR_DRIFT":
            self.state["sensor_health"] = max(0.15, 1.0 - (severity * 0.9))

        for key in ["fan_health", "pump_health", "comms_health", "sensor_health", "contactor_stability"]:
            self.state[key] = max(0.0, min(1.0, self.state[key]))

    def _compute_charge_request(self, current_time: datetime) -> float:
        if self.state["charger_state"] not in [self.STATE_CHARGING, self.STATE_THROTTLED]:
            return 0.0

        soc = float(self.state["ev_soc_percent"])
        base_current = float(self.state["base_requested_current_a"])

        if soc < 55.0:
            soc_factor = 1.0
        elif soc < 80.0:
            soc_factor = 1.0 - ((soc - 55.0) / 25.0) * 0.35
        else:
            soc_factor = max(0.12, 0.65 - ((soc - 80.0) / 20.0) * 0.53)

        request = base_current * soc_factor

        if self.state["active_anomaly"] == "BMS_CHATTER":
            severity = float(self.state["anomaly_severity"])
            low_freq = math.sin(current_time.timestamp() * (1.2 + severity * 1.8))
            high_freq = math.sin(current_time.timestamp() * (4.0 + severity * 3.5))
            chatter = 1.0 + (0.08 + severity * 0.3) * low_freq + severity * 0.08 * high_freq
            request *= max(0.15, chatter)

        if self.state["active_anomaly"] == "CONTACTOR_CHATTER":
            severity = float(self.state["anomaly_severity"])
            stability = float(self.state["contactor_stability"])
            if random.random() < severity * 0.02:
                request *= random.uniform(0.0, 0.5 + stability * 0.3)

        return max(0.0, request)

    def update_internal_state(
        self, delta_sec: float, current_time: datetime, global_state: dict[str, Any]
    ) -> None:
        ambient_temp = float(global_state.get("ambient_temp_c", 25.0))
        grid_voltage = float(global_state.get("current_grid_voltage", 480.0))
        self.state["simulation_uptime_sec"] += delta_sec

        self._determine_active_anomaly(delta_sec)
        self._apply_health_drift(delta_sec, current_time)

        previous_state = self.state["charger_state"]

        # Protection logic
        if self.state["power_module_temp_c"] >= 96.0 or self.state["cabinet_temp_c"] >= 88.0:
            self.state["charger_state"] = self.STATE_FAULT
            self.state["error_code"] = self.ERROR_OVERTEMP
            self.state["requested_current_a"] = 0.0
            self.state["output_current_a"] = 0.0
            self.state["current_power_kw"] = 0.0

        if self.state["charger_state"] == self.STATE_FAULT:
            self.state["warning_code"] = 0
            if self.state["cabinet_temp_c"] <= 42.0 and self.state["power_module_temp_c"] <= 48.0:
                self.state["charger_state"] = self.STATE_IDLE
                self.state["error_code"] = self.ERROR_NONE
                self.state["ev_soc_percent"] = 0.0
                self.state["session_duration_sec"] = 0.0

        if self.state["charger_state"] in [self.STATE_HANDSHAKING, self.STATE_CHARGING, self.STATE_THROTTLED]:
            self.state["session_duration_sec"] += delta_sec
            if self.state["charger_state"] == self.STATE_HANDSHAKING and self.state["session_duration_sec"] >= 4.0:
                self.state["charger_state"] = self.STATE_CHARGING

        if self.state["charger_state"] in [self.STATE_CHARGING, self.STATE_THROTTLED]:
            if self.state["ev_soc_percent"] >= 95.0 or (
                self.state["session_duration_sec"] >= self.state["target_session_duration_sec"]
            ):
                self.state["charger_state"] = self.STATE_IDLE
                self.state["requested_current_a"] = 0.0

        # Preventative thermal derating
        derate = 0.0
        module_temp = float(self.state["power_module_temp_c"])
        cabinet_temp = float(self.state["cabinet_temp_c"])
        connector_temp = float(self.state["cable_temp_c"])

        if module_temp > 72.0:
            derate = max(derate, min(0.8, (module_temp - 72.0) / 26.0))
        if cabinet_temp > 60.0:
            derate = max(derate, min(0.75, (cabinet_temp - 60.0) / 24.0))
        if connector_temp > 58.0:
            derate = max(derate, min(0.7, (connector_temp - 58.0) / 22.0))

        self.state["derate_level_pct"] = max(0.0, min(100.0, derate * 100.0))
        if self.state["charger_state"] in [self.STATE_CHARGING, self.STATE_THROTTLED]:
            self.state["charger_state"] = self.STATE_THROTTLED if derate > 0.01 else self.STATE_CHARGING

        # warning code = 1 thermal warning, 2 connector warning, 3 comms instability, 4 sensor drift
        warning_code = 0
        if cabinet_temp > 55.0 or module_temp > 68.0:
            warning_code = 1
        if self.state["connector_resistance_mohm"] > 1.2 or connector_temp > 55.0:
            warning_code = max(warning_code, 2)
        if self.state["active_anomaly"] == "BMS_CHATTER" and self.state["anomaly_severity"] > 0.35:
            warning_code = max(warning_code, 3)
        if self.state["active_anomaly"] == "SENSOR_DRIFT" and self.state["anomaly_severity"] > 0.35:
            warning_code = max(warning_code, 4)
        self.state["warning_code"] = warning_code

        requested_current = self._compute_charge_request(current_time)
        requested_current *= max(0.15, 1.0 - derate)
        self.state["requested_current_a"] = requested_current

        if self.state["charger_state"] == self.STATE_IDLE:
            self.state["requested_current_a"] = 0.0

        if self.state["charger_state"] != self.STATE_FAULT:
            response_rate = 2.8 * max(0.35, self.state["contactor_stability"])
            curr_diff = self.state["requested_current_a"] - self.state["output_current_a"]
            self.state["output_current_a"] += curr_diff * response_rate * delta_sec
            if abs(self.state["output_current_a"]) < 0.25:
                self.state["output_current_a"] = 0.0

        current = max(0.0, self.state["output_current_a"])
        output_voltage = 0.0
        if current > 0.5 and self.state["charger_state"] != self.STATE_FAULT:
            output_voltage = self.state["ev_base_voltage"] + (self.state["ev_soc_percent"] / 100.0 * 60.0)
            if self.state["ev_base_voltage"] >= 800.0:
                output_voltage += 20.0
            self.state["current_power_kw"] = (current * output_voltage) / 1000.0
            self.state["input_power_kw"] = self.state["current_power_kw"] / max(0.88, 0.965 - derate * 0.04)
            kwh_added = (self.state["current_power_kw"] * delta_sec) / 3600.0
            capacity = max(1.0, self.state["ev_battery_capacity_kwh"])
            self.state["ev_soc_percent"] = min(
                100.0, self.state["ev_soc_percent"] + (kwh_added / capacity) * 100.0
            )
        else:
            self.state["current_power_kw"] = 0.0
            self.state["input_power_kw"] = 0.6 if self.state["charger_state"] != self.STATE_FAULT else 0.2

        self.state["output_voltage_v"] = output_voltage
        self.state["grid_current_a"] = self.state["input_power_kw"] * 1000.0 / max(1.0, grid_voltage)
        self.state["dc_bus_voltage_v"] = 720.0 + (grid_voltage - 480.0) * 0.9 - derate * 18.0

        # Cooling and thermal dynamics
        fan_target = 0.0
        if self.state["charger_state"] in [self.STATE_CHARGING, self.STATE_THROTTLED, self.STATE_FAULT]:
            fan_target = max(600.0, min(4500.0, 900.0 + max(module_temp - 32.0, 0.0) * 95.0))
        fan_target *= max(0.0, self.state["fan_health"])

        pump_target = 0.0
        if current > 1.0 or self.state["charger_state"] == self.STATE_FAULT:
            pump_target = max(1200.0, min(3600.0, 1500.0 + current * 5.0 + max(connector_temp - 30.0, 0.0) * 20.0))
        pump_target *= max(0.0, self.state["pump_health"])

        self.state["cooling_fan_rpm"] += (fan_target - self.state["cooling_fan_rpm"]) * 1.4 * delta_sec
        self.state["pump_speed_rpm"] += (pump_target - self.state["pump_speed_rpm"]) * 1.0 * delta_sec
        self.state["coolant_flow_lpm"] += (
            ((self.state["pump_speed_rpm"] / 3600.0) * 18.0) - self.state["coolant_flow_lpm"]
        ) * 0.8 * delta_sec
        self.state["coolant_pressure_kpa"] = 150.0 + self.state["coolant_flow_lpm"] * 8.0

        module_heat_kw = (current / 220.0) ** 2 * 6.5
        resistive_multiplier = max(1.0, self.state["connector_resistance_mohm"] / 0.55)
        connector_heat_kw = (current / 220.0) ** 2 * 2.6 * resistive_multiplier
        if self.state["active_anomaly"] == "CONNECTOR_ARCING" and current > 5.0:
            flare = 1.0 + max(0.0, math.sin(current_time.timestamp() * 0.65)) * 2.2
            connector_heat_kw += flare * self.state["anomaly_severity"] * 2.0

        fan_cooling = (self.state["cooling_fan_rpm"] / 4500.0) * 8.0
        liquid_cooling = (self.state["coolant_flow_lpm"] / 18.0) * 7.0

        self.state["power_module_temp_c"] += (
            module_heat_kw - fan_cooling * 0.55 - liquid_cooling * 0.42 - (self.state["power_module_temp_c"] - ambient_temp) * 0.04
        ) * delta_sec

        self.state["coolant_inlet_temp_c"] += (
            (ambient_temp + 2.0 + max(self.state["cabinet_temp_c"] - ambient_temp, 0.0) * 0.08) - self.state["coolant_inlet_temp_c"]
        ) * 0.18 * delta_sec
        outlet_target = self.state["coolant_inlet_temp_c"] + module_heat_kw * 1.4 + connector_heat_kw * 0.5
        outlet_target -= (self.state["coolant_flow_lpm"] / 18.0) * 4.0
        self.state["coolant_outlet_temp_c"] += (outlet_target - self.state["coolant_outlet_temp_c"]) * 0.35 * delta_sec

        self.state["cable_temp_c"] += (
            connector_heat_kw - liquid_cooling * 0.32 - (self.state["cable_temp_c"] - ambient_temp) * 0.055
        ) * delta_sec

        cabinet_heat = module_heat_kw * 0.42 + self.state["input_power_kw"] * 0.02
        self.state["cabinet_temp_c"] += (
            cabinet_heat - fan_cooling * 0.68 - (self.state["cabinet_temp_c"] - ambient_temp) * 0.05
        ) * delta_sec

        # prevent unphysical underflow
        for temp_key in [
            "cabinet_temp_c",
            "power_module_temp_c",
            "cable_temp_c",
            "coolant_inlet_temp_c",
            "coolant_outlet_temp_c",
        ]:
            self.state[temp_key] = max(ambient_temp - 3.0, self.state[temp_key])

        if previous_state != self.state["charger_state"]:
            logger.info(
                "%s state %s -> %s (warning=%s error=%s derate=%.1f%%)",
                self.name,
                previous_state,
                self.state["charger_state"],
                self.state["warning_code"],
                self.state["error_code"],
                self.state["derate_level_pct"],
            )

    def _apply_sensor_drift(self, value: float, sensor_group: str) -> float:
        if self.state["active_anomaly"] != "SENSOR_DRIFT":
            return value

        severity = float(self.state["anomaly_severity"])
        drift_sign = 1.0 if hash(f"{self.name}:{sensor_group}") % 2 == 0 else -1.0
        group_scale = {
            "temperature": 3.5,
            "current": 8.0,
            "voltage": 6.0,
            "flow": 1.2,
            "ratio": 6.0,
        }.get(sensor_group, 1.0)
        return value + drift_sign * severity * group_scale

    def read_sensor(self, sensor_name: str, global_state: dict[str, Any]) -> float:
        is_active = self.state["charger_state"] in [
            self.STATE_HANDSHAKING,
            self.STATE_CHARGING,
            self.STATE_THROTTLED,
        ]
        grid_voltage = float(global_state.get("current_grid_voltage", 480.0))

        match sensor_name:
            case "Grid_Voltage_AC":
                value = grid_voltage + random.gauss(0, 0.7)
                value = self._apply_sensor_drift(value, "voltage")
                return round(value, 2)
            case "Grid_Current_AC":
                value = self.state["grid_current_a"] + random.gauss(0, 0.15)
                value = self._apply_sensor_drift(value, "current")
                return round(max(0.0, value), 2)
            case "Input_Power_kW":
                value = self.state["input_power_kw"] + random.gauss(0, 0.08)
                return round(max(0.0, value), 2)
            case "DC_Bus_Voltage":
                value = self.state["dc_bus_voltage_v"] + random.gauss(0, 1.0)
                value = self._apply_sensor_drift(value, "voltage")
                return round(value, 2)
            case "Output_Voltage_DC":
                if not is_active:
                    return 0.0
                value = self.state["output_voltage_v"] + random.gauss(0, 0.6)
                value = self._apply_sensor_drift(value, "voltage")
                return round(max(0.0, value), 2)
            case "Output_Current_DC":
                if self.state["output_current_a"] < 0.5:
                    return 0.0
                noise = 0.45
                if self.state["active_anomaly"] == "CONNECTOR_ARCING":
                    noise += 0.8 + self.state["anomaly_severity"] * 4.5
                value = self.state["output_current_a"] + random.gauss(0, noise)
                value = self._apply_sensor_drift(value, "current")
                return round(max(0.0, value), 2)
            case "Requested_Current_DC":
                value = self.state["requested_current_a"]
                value = self._apply_sensor_drift(value, "current")
                return round(max(0.0, value), 2)
            case "Cooling_Fan_RPM":
                rpm = self.state["cooling_fan_rpm"]
                return round(max(0.0, rpm + random.gauss(0, max(6.0, rpm * 0.015))), 0)
            case "Pump_Speed_RPM":
                rpm = self.state["pump_speed_rpm"]
                return round(max(0.0, rpm + random.gauss(0, max(8.0, rpm * 0.012))), 0)
            case "Coolant_Flow_LPM":
                value = self.state["coolant_flow_lpm"] + random.gauss(0, 0.08)
                value = self._apply_sensor_drift(value, "flow")
                return round(max(0.0, value), 2)
            case "Coolant_Pressure_kPa":
                value = self.state["coolant_pressure_kpa"] + random.gauss(0, 1.0)
                return round(max(120.0, value), 1)
            case "Coolant_Inlet_Temp":
                value = self.state["coolant_inlet_temp_c"] + random.gauss(0, 0.08)
                value = self._apply_sensor_drift(value, "temperature")
                return round(value, 2)
            case "Coolant_Outlet_Temp":
                value = self.state["coolant_outlet_temp_c"] + random.gauss(0, 0.1)
                value = self._apply_sensor_drift(value, "temperature")
                return round(value, 2)
            case "Ambient_Temp":
                value = float(global_state.get("ambient_temp_c", 25.0)) + random.gauss(0, 0.2)
                return round(value, 2)
            case "Cabinet_Temp":
                value = self.state["cabinet_temp_c"] + random.gauss(0, 0.08)
                value = self._apply_sensor_drift(value, "temperature")
                return round(value, 2)
            case "Power_Module_Temp":
                value = self.state["power_module_temp_c"] + random.gauss(0, 0.08)
                value = self._apply_sensor_drift(value, "temperature")
                return round(value, 2)
            case "Cable_Connector_Temp":
                value = self.state["cable_temp_c"] + random.gauss(0, 0.1)
                value = self._apply_sensor_drift(value, "temperature")
                return round(value, 2)
            case "Connector_Resistance_mOhm":
                value = self.state["connector_resistance_mohm"] + random.gauss(0, 0.02)
                return round(max(0.1, value), 3)
            case "Derate_Level_Percent":
                value = self.state["derate_level_pct"]
                value = self._apply_sensor_drift(value, "ratio")
                return round(max(0.0, min(100.0, value)), 1)
            case "EV_State_of_Charge":
                return round(self.state["ev_soc_percent"], 1) if is_active else 0.0
            case "Charger_State":
                return float(self.state["charger_state"])
            case "Session_Duration":
                return round(self.state["session_duration_sec"], 0) if is_active else 0.0
            case "Warning_Code":
                return float(self.state["warning_code"])
            case "Error_Code":
                return float(self.state["error_code"])
            case _:
                return 0.0
