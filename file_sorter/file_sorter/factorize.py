import logging
from time import time
from multiprocessing import Pool, current_process,  cpu_count

logger = logging.getLogger()
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)


def worker(x):
    logger.debug(f"pid={current_process().pid} started, x={x}")
    y = [1]
    for i in range(2, x):
        if x % i == 0:
            y.append(i)
    if x!=1:
        y.append(x)
    logger.debug(f"pid={current_process().pid} finished, len(y)={len(y)}")
    return y


def factorize(*number):
    start_time = time()

    logger.debug(f'pid={current_process().pid} Main thread started.')

    
    cpus = cpu_count()
    if cpus > len(number):
        cpus = len(number)
    else:
        cpus -=1
    # cpus = 1
    print(cpus)

    with Pool(processes=cpus) as pool:
        result = pool.map(worker, number)
    
    logger.debug(
        f'pid={current_process().pid} Main thread finished after {time()-start_time} seconds')

    return result


if __name__ == '__main__':
    a, b, c, d  = factorize(128, 255, 99999, 10651060)

    assert a == [1, 2, 4, 8, 16, 32, 64, 128]
    assert b == [1, 3, 5, 15, 17, 51, 85, 255]
    assert c == [1, 3, 9, 41, 123, 271, 369, 813, 2439, 11111, 33333, 99999]
    assert d == [1, 2, 4, 5, 7, 10, 14, 20, 28, 35, 70, 140, 76079, 152158, 304316, 380395, 532553, 760790, 1065106, 1521580, 2130212, 2662765, 5325530, 10651060]
