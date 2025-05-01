import time
from datetime import datetime


def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()  # 시작 시간 기록
        print(
            f"( Func '{func.__name__}' started at {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')} ) \n"
        )
        result = func(*args, **kwargs)  # 함수 실행
        end_time = time.time()  # 종료 시간 기록
        print(
            f"( Func '{func.__name__}' ended at {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')} ) \n"
        )
        elapsed_time = end_time - start_time  # 실행 시간 계산
        print(f"( Func '{func.__name__}' executed in {elapsed_time:.6f} seconds ) \n")
        return result

    return wrapper
