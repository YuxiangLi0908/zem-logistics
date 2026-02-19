class AppConfig:
    def __init__(self) -> None:
        self.WAREHOUSE_ADDRESS = {
            "LA-91761": {
                "name": "ZEM LOGISTICS INC",
                "address1": "5450 E Francis St",
                "city": "Ontario",
                "regionCode": "CA",
                "postalCode": "91761",
                "countryCode": "US",
            },
            "NJ-07001": {
                "name": "ZEM LOGISTICS INC",
                "address1": "27 Engelhard Ave",
                "city": "Avenel",
                "regionCode": "NJ",
                "postalCode": "07001",
                "countryCode": "US",
            },
            "SAV-31326": {
                "name": "ZEM LOGISTICS INC",
                "address1": "1001 Trade Center Pkwy",
                "city": "Rincon",
                "regionCode": "GA",
                "postalCode": "31326",
                "countryCode": "US",
            },
        }

app_config = AppConfig()
