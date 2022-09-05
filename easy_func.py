import asyncio


def chunk_list(_list: list, n: int) -> list:
    """ Chunks list into list of lists,
    _list = [1,2,3,4,5,6,7,8,9] n=3
    returns [[1,2,3],[4,5,6],[7,8,9]]

    Args:
        _list (list): list to be chunked
        n (int): chunk size

    Returns:
        list: list of lists
    """
    return [_list[i:i+n] for i in range(0, len(_list), n)]


def execute_async_tasks(tasks: list) -> list:
    """ Asynchronous execution of the coroutine list 

    Args:
        tasks (list): list of coroutines

    Returns:
        list: list of events
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = asyncio.gather(*tasks)
    loop.run_until_complete(results)
    loop.close()
    return results.result()


def count_dict_nested_values(dictionary: dict, counter: int=0)->int:
    """ Function for calculating the number of values in the dictionary,
    including nested values

    Args:
        dictionary (dict): input dictionary to values counting
        counter (int, optional): start counter number. Defaults to 0.

    Returns:
        int: number of counted values 
    """
    for key in dictionary:
        if isinstance(dictionary[key], dict):
            counter = count_dict_nested_values(dictionary[key], counter)
        else:
            counter += 1
    return counter
