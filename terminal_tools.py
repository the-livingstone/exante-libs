#!/usr/bin/env python3

import re
import os
import colorama as clr
from tabulate import tabulate
from simple_term_menu import TerminalMenu as TM

    
def clear():
    os.system('clear')

def colorize(text, sym_type='STOCK', expired=False, is_trading=True, state=''):
    type_colors = {
        'STOCK': clr.Fore.WHITE,
        'FX_SPOT': clr.Fore.WHITE,
        'FOREX': clr.Fore.WHITE,
        'FUTURE': clr.Fore.GREEN,
        'FUND': clr.Fore.WHITE,
        'SPREAD': clr.Fore.YELLOW,
        'CALENDAR_SPREAD': clr.Fore.YELLOW,
        'OPTION': clr.Fore.CYAN,
        'BOND': clr.Fore.MAGENTA,
        'CFD': clr.Fore.BLUE,
    }
    status_colors = {
        'new': clr.Back.GREEN + clr.Style.BRIGHT,
        'used_ne': clr.Back.YELLOW + clr.Style.BRIGHT,
        'removed': clr.Fore.BLACK + clr.Back.WHITE + clr.Style.BRIGHT,
        'highlighted': clr.Fore.BLACK + clr.Back.WHITE,
        'missing': clr.Fore.RED +clr.Back.WHITE + clr.Style.DIM,
        'invalid': clr.Fore.RED + clr.Style.BRIGHT
    }
    modifiers = {
        'reset': clr.Style.RESET_ALL,
        'bright': clr.Style.BRIGHT,
        'dim': clr.Style.DIM,

    }
    if not state:
        if not expired and is_trading:
            colored = type_colors[sym_type] + modifiers['bright'] + text
        elif is_trading:
            colored = type_colors[sym_type] + text
        else:
            colored = type_colors[sym_type] + modifiers['dim'] + text
    elif state in status_colors:
        colored = status_colors[state] + text
    else:
        return text
    return colored + modifiers['reset']

def pick_from_list_tm(
    options_list: list,
    option_name='options',
    message: str = '',
    specify='',
    preview=None,
    preview_size=0.8,
    cursor_index=None,
    clear_screen=True):
    
    menu_items = list()
    if not options_list:
        return None
    if len(options_list) == 1:
        return 0
    for item in options_list:
        if isinstance(item, dict) and len(item) == 1:
            menu_items.append(item.keys()[0])
        elif isinstance(item, (list, tuple)):
            menu_items.append(item[0])
        elif isinstance(item, (str, int, float, bool)):
            menu_items.append(str(item))
        else:
            menu_items.append('<bad item>')
    if specify == '.' and '.' in menu_items:
        return menu_items.index('.')
    elif specify == '..' and '..' in menu_items:
        return menu_items.index('..')
    if specify:
        if specify.isdecimal():
            return int(specify) - 1
        
        select_one = [x for x in options_list if specify in x]
        if len(select_one) == 1:
            return options_list.index(select_one[0])

        if not select_one:
            select_one = [x for x in menu_items if specify in x]
        if len(select_one) == 1:
            return menu_items.index(select_one[0])
    menu_title = f'{message}\n\nI\'ve found several {option_name}:'
    search_hint = 'select item with arrows or type "/" to search'
    menu_cursor = '→ '

    main_menu_cursor_style = ("fg_red", "bold")
    highlight = ("bg_gray", "fg_black", "bold")
    all_good = False
    while not all_good:
        try:
            selected = TM(
                menu_items,
                title=menu_title,
                menu_cursor=menu_cursor,
                menu_cursor_style=main_menu_cursor_style,
                menu_highlight_style=highlight,
                cycle_cursor=True,
                clear_screen=clear_screen,
                show_search_hint=True,
                show_search_hint_text=search_hint,
                search_key=None,
                preview_command=preview,
                preview_size=preview_size,
                cursor_index=cursor_index
            ).show()
            all_good = True
        except ValueError:
            continue
    return selected

def pick_from_list(options_list: list, option_name='options', message: str = '', specify='', color=False):
    selected = None
    try_again = 'y'
    while try_again != 'n' and selected is None:
        search_results = {}
        options_str = []
        if type(options_list) != list:
            print('(o_O) ???')
            print(f'Bad input data type ({type(options_list)}), should be a list')
            break
        for num, s in enumerate(options_list):
            if type(s) == str:
                options_str.append(s)
            elif type(s) == tuple:
                if color:
                    options_str.append(colorize(s[0], sym_type=s[2], expired=s[3]))
                else:
                    options_str.append(s[0])
            elif type(s) == dict and len(s) == 1:
                options_str.append(sorted(s.keys())[0])
            elif type(s) == dict and s.get('columns'):
                if color:
                    s['columns'][0] = colorize(s['columns'][0], sym_type=s['symbol_type'], expired=s['is_expired'])
                    if len(s['columns']) > 1 and s.get('highlight'):
                        highlighted = colorize(s['columns'][1][s['highlight'][0]:s['highlight'][1]], state='highlighted')
                        s['columns'][1] = f"{s['columns'][1][:s['highlight'][0]]}{highlighted}{s['columns'][1][s['highlight'][1]:]}"
                options_str.append(s['columns'])
            else:
                print('(o_O) ???')
                print(f'Bad input list entry type: {num}, {type(s)}')
                return None
        if len(options_list) == len(options_str):
            if isinstance(options_str[0], str):
                search_results = [(num + 1, r) for num, r in enumerate(options_str)]
            elif isinstance(options_str[0], list):
                search_results = [(num + 1, *r) for num, r in enumerate(options_str)]
                
        else:
            print('Smth is wrong, sorry')
            return None
        while len(search_results) > 1:
            clear()
            print(message)
            filtrator = list()
            if not specify:
                print(tabulate(search_results))
                try:
                    specify = input(f'I\'ve found several {option_name}, type number in list or part of name: ')
                except KeyboardInterrupt:
                    print()
                    print('(×_×)')
                    exit(0)
            if specify == '.' and '.' in options_str:
                search_results = [x for x in search_results if x[1] == '.']
            elif specify == '..' and '..' in options_str:
                search_results = [x for x in search_results if x[1] == '..']
            elif specify.isdecimal():
                if int(specify) in [x[0] for x in search_results]:
                    search_results = [x for x in search_results if x[0] == int(specify)]
                    break
                else:
                    specify = ''
            for item in search_results:
                if specify in item[1]:
                    filtrator.append(item)
            if not filtrator:
                specify = specify.upper()
                for item in search_results:
                    if specify in item[1]:
                        filtrator.append(item)
            search_results = filtrator
            specify = ''
        if not search_results:
            try:
                try_again = input('Nothing is found... Try again? (y/n): ')
            except KeyboardInterrupt:
                print()
                print('(×_×)')
                exit(0)
        else:
            selected = next((x[0] for x in search_results), None)
            if selected is not None:
                selected -= 1
    if selected is not None:
        print(f'Selected: {options_str[selected][0]}')
        return selected
    else:
        print('Nothing selected')
        return None

def sorting_expirations(expiration):
    re_color = r'\x1b\[\d+m'
    re_short = r'[FGHJKMNQUVXZ]\d{4}'
    re_long = r'\d{1,2}[FGHJKMNQUVXZ]\d{4}'
    re_spread = r'[FGHJKMNQUVXZ]\d{4}-[FGHJKMNQUVXZ]\d{4}'
    re_cont = r'CONT|PERP'
    while re.match(re_color, expiration):
        expiration = expiration[re.match(re_color, expiration).end():]
    if re.search(re_spread, expiration):
        match = re.search(re_spread, expiration)
        ticker_exchange = expiration[:match.start()]
        near_mat = f'{expiration[match.end()+1:match.end()+4]}{match.end()}'
        far_mat = f'{expiration[match.end()+7:match.end()+10]}{match.end()+6}'
        return(f'{ticker_exchange}.{near_mat}-{far_mat}')
    elif re.search(re_long, expiration):
        match = re.search(re_long, expiration)
        under_ten = 7 + match.start() - match.end()
        ticker_exchange = expiration[:match.start()]
        if under_ten:
            maturity = f'{expiration[match.start()+2:match.end()]}.{expiration[match.start()+1]}.0{expiration[match.start()]}'
        else:
            maturity = f'{expiration[match.start()+3:match.end()]}.{expiration[match.start()+2]}.{expiration[match.start():match.start()+2]}'
        return(f'{ticker_exchange}.{maturity}')
    elif re.search(re_short, expiration):
        match = re.search(re_short, expiration)
        ticker_exchange = expiration[:match.start()]
        return(f'{ticker_exchange}.{expiration[match.start()+1:match.end()]}.{expiration[match.start()]}')
    elif re.search(re_cont, expiration):
        match = re.search(re_cont, expiration)
        ticker_exchange = expiration[:match.start()]
        return(f'{ticker_exchange}.ZZZ')

    else:
        return(expiration)

if __name__ == '__main__':
    print("Terminal tools by alser")