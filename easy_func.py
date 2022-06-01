import asyncio


def chunk_list(_list, n):
    """
    :param list: list to be chunked
    :param n: chunk size
    chunks list into list of lists,
    _list = [1,2,3,4,5,6,7,8,9] n=3
    returns [[1,2,3],[4,5,6],[7,8,9]]
    """
    return [_list[i:i+n] for i in range(0, len(_list), n)]


def execute_async_tasks(tasks: list)->list:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = asyncio.gather(*tasks)
    loop.run_until_complete(results)
    loop.close()
    return results.result()
