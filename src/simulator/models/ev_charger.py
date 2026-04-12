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
            # Mechanical & Cooling (2Hz)
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
        ]

        self.state = {
            "charger_state": 0,  # 0=Idle, 1=Handshaking, 2=Charging, 3=Throttled, 4=Fault
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
        }

    def start_session(self, duration_sec: float) -> None:
        """Site controller calls this when a car plugs in."""
        self.state["charger_state"] = 1  # Start in Handshaking
        self.state["session_duration_sec"] = 0.0

        # Randomize the arriving vehicle's specs
        self.state["ev_battery_capacity_kwh"] = random.uniform(60.0, 100.0)
        self.state["ev_soc_percent"] = random.uniform(10.0, 40.0)  # Arrives with low battery
        self.state["ev_base_voltage"] = random.choice([400.0, 800.0])  # 400V or 800V architecture

        # Max request depends on the car's architecture
        self.state["requested_current_a"] = (
            200.0 if self.state["ev_base_voltage"] == 400.0 else 350.0
        )

    def update_internal_state(
        self, delta_sec: float, current_time: datetime, global_state: dict
    ) -> None:
        ambient_temp = global_state.get("ambient_temp_c", 25.0)

        # 1. State Machine & Time
        if self.state["charger_state"] in [1, 2, 3]:
            self.state["session_duration_sec"] += delta_sec

            # Handshake takes 5 seconds, then begin charging
            if self.state["charger_state"] == 1 and self.state["session_duration_sec"] > 5.0:
                self.state["charger_state"] = 2

            # Finish if SOC hits 95%
            if self.state["ev_soc_percent"] >= 95.0:
                self.state["charger_state"] = 0
                self.state["requested_current_a"] = 0.0

        # 2. Thermal Protection Logic (Throttling)
        # If cabinet gets too hot, drop into state 3 and reduce requested current
        if self.state["charger_state"] in [2, 3]:
            if self.state["cabinet_temp_c"] > 65.0:
                self.state["charger_state"] = 3
                self.state["requested_current_a"] = 50.0  # Heavy throttle
            elif self.state["cabinet_temp_c"] < 55.0 and self.state["charger_state"] == 3:
                self.state["charger_state"] = 2
                self.state["requested_current_a"] = 200.0  # Resume full power

        # 3. Electrical Math
        if self.state["charger_state"] == 0:
            self.state["requested_current_a"] = 0.0

        curr_diff = self.state["requested_current_a"] - self.state["output_current_a"]
        self.state["output_current_a"] += curr_diff * 2.0 * delta_sec
        current = self.state["output_current_a"]

        dc_voltage = 0.0
        if current > 1.0:
            # Simulate Li-ion charge curve: Voltage rises as SOC increases
            dc_voltage = self.state["ev_base_voltage"] + (
                self.state["ev_soc_percent"] / 100.0 * 50.0
            )

            # Calculate power and increment Battery SOC
            self.state["current_power_kw"] = (current * dc_voltage) / 1000.0
            kwh_added = (self.state["current_power_kw"] * delta_sec) / 3600.0
            self.state["ev_soc_percent"] += (
                kwh_added / self.state["ev_battery_capacity_kwh"]
            ) * 100.0
        else:
            self.state["current_power_kw"] = 0.0

        # 4. Thermal & Mechanical Math
        heat_generated = (current / 200.0) ** 2 * 6.0

        # Fans (Air Cooling for Cabinet)
        target_rpm = max(0.0, min(4500.0, (self.state["cabinet_temp_c"] - 30.0) * 300.0))
        self.state["cooling_fan_rpm"] += (
            (target_rpm - self.state["cooling_fan_rpm"]) * 1.0 * delta_sec
        )
        active_air_cooling = (self.state["cooling_fan_rpm"] / 4500.0) * 8.0

        self.state["cabinet_temp_c"] += (
            heat_generated
            - active_air_cooling
            - ((self.state["cabinet_temp_c"] - ambient_temp) * 0.05)
        ) * delta_sec

        # Liquid Cooling (For the Cable)
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

        match sensor_name:
            # Electrical
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
                return round(self.state["output_current_a"] + random.gauss(0, 0.5), 2)
            case "Requested_Current_DC":
                return round(self.state["requested_current_a"], 2)

            # Mechanical & Cooling
            case "Cooling_Fan_RPM":
                rpm = self.state["cooling_fan_rpm"]
                return round(rpm + random.gauss(0, rpm * 0.02) if rpm > 10 else 0, 0)
            case "Coolant_Flow_LPM":
                lpm = self.state["coolant_flow_lpm"]
                return round(lpm + random.gauss(0, lpm * 0.05) if lpm > 0.5 else 0, 2)
            case "Coolant_Pressure_kPa":
                # Pressure is roughly proportional to flow squared, plus base system pressure (150 kPa)
                if self.state["coolant_flow_lpm"] < 0.5:
                    return 150.0
                pressure = 150.0 + (self.state["coolant_flow_lpm"] ** 2 * 0.8)
                return round(pressure + random.gauss(0, 2.0), 1)

            # Thermal
            case "Ambient_Temp":
                return round(global_state.get("ambient_temp_c", 25.0) + random.gauss(0, 0.2), 2)
            case "Cabinet_Temp":
                return round(self.state["cabinet_temp_c"] + random.gauss(0, 0.1), 2)
            case "Cable_Connector_Temp":
                return round(self.state["cable_temp_c"] + random.gauss(0, 0.1), 2)

            # State
            case "EV_State_of_Charge":
                if not is_active:
                    return 0.0
                return round(self.state["ev_soc_percent"], 1)
            case "Charger_State":
                return float(self.state["charger_state"])
            case "Session_Duration":
                if not is_active: return 0.0
                return round(self.state["session_duration_sec"], 0)
            case _:
                return 0.0