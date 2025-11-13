from datetime import datetime
import time


def run():
    print(f"Job running at {datetime.now()}")
    time.sleep(5)
    print("Job finished.")

if __name__ == "__main__":
    run()