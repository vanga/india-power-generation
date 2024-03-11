#### State real time generation
```
curl 'https://meritindia.in/StateWiseDetails/BindCurrentStateStatus' \
  -H 'Accept: application/json, text/javascript, */*; q=0.01' \
  -H 'Content-Type: application/json; charset=UTF-8' \
  --data-raw '{StateCode:"MHA"}'
```
```json
[{"Demand":"28,197","ISGS":"18,923","ImportData":"9,274"}]
```

#### State's total daily generation
```
curl 'https://meritindia.in/StateWiseDetails/GetStateWiseDetailsForPiChart' \
  -H 'Accept: application/json, text/javascript, */*; q=0.01' \
  -H 'Content-Type: application/json; charset=UTF-8' \
  --data-raw '{StateCode:"mha",date:"08 Mar 2024"}'
```
```json
[
    {
        "TypeOfEnergy": "State Generation",
        "EnergyValue": "311243"
    },
    {
        "TypeOfEnergy": "Central ISGS",
        "EnergyValue": "112850.65"
    },
    {
        "TypeOfEnergy": "Other ISGS",
        "EnergyValue": "1147.85"
    },
    {
        "TypeOfEnergy": "Bilateral",
        "EnergyValue": null
    },
    {
        "TypeOfEnergy": "Power Exchange",
        "EnergyValue": null
    }
]
```

#### State's plant wise generation
```
curl 'https://meritindia.in/StateWiseDetails/GetPowerStationData' \
  -H 'Accept: application/json, text/javascript, */*; q=0.01' \
  -H 'Content-Type: application/json; charset=UTF-8' \
  --data-raw '{StateCode:"mha",date:"08 Mar 2024"}'
```
Sample response
```json
[
    {
        "PowerStationName": "DAHANU SOLAR POWER PRIVATE LIMITED (DSPPL)",
        "NonSchedule": "0.00",
        "Schedule": "0",
        "ChartShowingScheduleValue": "0",
        "ChartShowingNonScheduleValue": "0.00",
        "TypeOfGeneration": "Renewable"
    },
    {
        "PowerStationName": "SOLAR",
        "NonSchedule": "-427.68",
        "Schedule": "21516",
        "ChartShowingScheduleValue": "21516",
        "ChartShowingNonScheduleValue": "-427.68",
        "TypeOfGeneration": "Renewable"
    }
]
```