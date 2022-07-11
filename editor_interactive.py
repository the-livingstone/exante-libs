#!/usr/bin/env python3
import asyncio 
import datetime as dt
import logging
import operator
from copy import copy, deepcopy
from functools import reduce
from pprint import pp, pformat
from typing import Union
from libs import sdb_schemas
from libs import sdb_schemas_cprod as cdb_schemas
from libs.async_symboldb import SymbolDB
from libs.async_sdb_additional import SDBAdditional, SdbLists
from libs.sdb_schemas import type_mapping
from libs.terminal_tools import pick_from_list_tm, clear, StatusColor


class EditInstrument:
    schemas = {
        'BOND': sdb_schemas.BondSchema,
        'STOCK': sdb_schemas.StockSchema,
        'FUTURE': sdb_schemas.FutureSchema,
        'FX_SPOT': sdb_schemas.FxSpotSchema,
        'FOREX': sdb_schemas.ForexSchema,
        'OPTION': sdb_schemas.OptionSchema,
        'SPREAD': sdb_schemas.SpreadSchema,
        'CALENDAR_SPREAD': sdb_schemas.CalendarSpreadSchema,
        'CFD': sdb_schemas.CfdSchema,
        'FUND': sdb_schemas.FundSchema
    }
    cschemas = {
        'BOND': cdb_schemas.BondSchema,
        'STOCK': cdb_schemas.StockSchema,
        'FUTURE': cdb_schemas.FutureSchema,
        'FX_SPOT': cdb_schemas.FxSpotSchema,
        'FOREX': cdb_schemas.ForexSchema,
        'OPTION': cdb_schemas.OptionSchema,
        'SPREAD': cdb_schemas.SpreadSchema,
        'CALENDAR_SPREAD': cdb_schemas.CalendarSpreadSchema,
        'CFD': cdb_schemas.CfdSchema,
        'FUND': cdb_schemas.FundSchema
    }
    always_exclude = [
        '_id',
        '_rev',
        '_creationTime',
        '_lastUpdateTime',
        'isAbstract',
        'gatewayId',
        'accountId',
        'providerId',
        'EXANTEId'
    ]

    def __init__(
            self,
            exanteid: str = None,
            compiled_instr: dict = None,
            instrument_type: str = None,
            env: str = 'prod',
            input_list: list = [],
            final_update_list: list = [],
            sdb: SymbolDB = None,
            sdbadds: SDBAdditional = None
        ) -> None:
        self.path = []
        self.final_update_list = final_update_list
        self.input_list = input_list
        self.exanteid = exanteid if exanteid else 'root'
        self.instrument = compiled_instr if compiled_instr else {}
        self.sdb = sdb if sdb else SymbolDB(env)
        self.sdbadds = sdbadds if sdbadds else SDBAdditional(env)
        self.modify_copy = deepcopy(compiled_instr)
        if instrument_type:
            self.instrument_type = instrument_type
        else:
            self.instrument_type = compiled_instr.get('type')
        if env == 'prod':
            self.schema = self.schemas[self.instrument_type]
            self.navi = sdb_schemas.SchemaNavigation(self.schema)
        elif env == 'cprod':
            self.schema = self.cschemas[self.instrument_type]
            self.navi = cdb_schemas.SchemaNavigation(self.schema)

    @property
    def logger(self):
        return logging.getLogger(f"{self.__class__.__name__}")

    # dunno what's that 
    def __menu_preview(self, entry):
        show_part = self.__get_part()
        try:
            show_part = show_part[entry]
        except KeyError:
            return None
        parent = next((x for x in reversed(self.path + [entry]) if isinstance(x, str)), None)
        data = self.sdbadds.fancy_dict(show_part, parent=parent, recursive=1, depth=[])
        return [''.join(d[0]) + d[1][0] for d in data]

    def __get_part(self, path: list = None):
        """
        wrapper method to go into nested dict/list structure by given path
        :param path: list of dict keys and list indices
        :return: part of given instrument
        """
        if path is None:
            path = self.path
        try:
            return reduce(operator.getitem, path, self.modify_copy)
        except Exception as e:
            return e

    def __navigate(self, path: list = None, get_list: bool = False) -> Union[str, list]:
        """
        method to form a list or printable string of current instrument part path,
        substituting uuids and indices of account/gateway lists with readable names
        :param path: list of dict keys and list indices
        :param get_list: return list of names instead of formatted string
        :return: formatted printable string (default) or the list of same names
        """
        nav_list = [self.exanteid]
        # navigate
        if path is None:
            path = self.path
        for i, p in enumerate(path):
            humanized = None
            # if p in ['account', 'gateway']:
            #     humanized = next(x[0] for x in self.sdbadds.get_list_from_sdb(SdbLists.'{.value}s')
            #                 if x[1] == self.__get_part(path[:i])[f'{p}Id'])
            if i and path[i - 1] == 'providerOverrides':
                ovr_list = asyncio.run(
                    self.sdbadds.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value)
                )
                ovr_list.extend(
                    asyncio.run(
                        self.sdbadds.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value)
                    )
                )
                humanized = next(x[0] for x in ovr_list if x[1] == p)
                nav_list.append(humanized)
            elif isinstance(p, int) and path[i-1] == 'accounts':
                humanized = next(
                    x[0] for x
                    in asyncio.run(self.sdbadds.get_list_from_sdb(SdbLists.ACCOUNTS.value))
                    if x[1] == self.__get_part(path[:i+1])['accountId']
                )
                nav_list.append(humanized)
            elif isinstance(p, int) and path[i-1] == 'gateways':
                humanized = next(
                    x[0] for x
                    in asyncio.run(self.sdbadds.get_list_from_sdb(SdbLists.GATEWAYS.value))
                    if x[1] == self.__get_part(path[:i+1])['gatewayId']
                )
                nav_list.append(humanized)
            else:
                nav_list.append(str(p))
        if not get_list:
            return ' > '.join(nav_list)
        else:
            return nav_list

    def __get_properties(self, path: list = None, **kwargs) -> Union[dict, None]:
        """
        method to get info of instrument part by given path,
        pydantic schema format
        :param path: list of dict keys and list indices
        :param kwargs: additional info about field type
        :return: dict of part info
        """
        if path is None:
            path = self.path
        try:
            props = self.navi.schema_lookup(path, **kwargs)
        except KeyError as e:
            print(e)
            return None
        if not path:
            return props[0]
        elif len(props) == 1:
            return props[0]
        elif len(props) > 1: # anyOf case
            the_part = self.__get_part(path)
            choices = []
            for p in props:
                option = (p['type'], p['type']) if p['type'] != 'object' \
                    else (f"${p.get('title')}", p['type'])
                choices.append(option)
            if isinstance(the_part, KeyError):
                selected = pick_from_list_tm(
                    choices,
                    f"possible field types for {path[-1]}",
                    clear_screen=False
                )
                if selected is None:
                    return None
                return props[selected]
            elif isinstance(the_part, TypeError):
                self.logger.error(f"Bad access. Used path: {'/'.join([str(x) for x in path])}")
                return None
            elif isinstance(the_part, (dict, list)):
                part_fitting = next((
                    x for x in props if x.get('type') in ['object', 'array']
                ), None)
                if not part_fitting:
                    return None
                return part_fitting
            else:
                part_fitting = next((
                    x for x in props if x.get('type') not in ['object', 'array']
                ), None)
                if not part_fitting:
                    return None
                return part_fitting

    def __ls(self, path: list = None, recurrency: int = 3) -> None:
        """
        method to fancy_print the instrument part by given path with appropriate depth
        :param path: list of dict keys and list indices
        :param recurrency: display depth of given part        
        """
        if path is None:
            path = self.path
        show_part = self.__get_part(path)
        parent = next((
            x for x in reversed(path)
            if isinstance(x, str)
            and x not in [
                y[1] for y
                in (
                    asyncio.run(
                        self.sdbadds.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value)
                    )
                    + asyncio.run(
                        self.sdbadds.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value)
                    )
                )
            ]
        ), None)
        colors = {x: StatusColor.INVALID for x in self.highlight}
        colors.update({'None': StatusColor.MISSING})
        self.sdbadds.fancy_print(
            show_part,
            exclude=self.always_exclude, 
            parent=parent, 
            recursive=recurrency,
            sort=True,
            colors=colors
        )

    def __general_input(self, message: str, header: str = None, delimiter: str = '/') -> Union[list, str]:
        """
        handler to accept typing input (opposed to menu selecting)
        :param message: the string dicrectly preceeding user input, e.g. 'type here: '
        :param header: larger piece of text at the top of the screen
        :param delimiter: delimiter to divide input string to the list of commands
        :return: list of commands divided by delimiter or None if input is '..'

        """
        if header is None:
            header = ''
        while True:
            clear()
            print(header)
            print()
            print(
                'Type "." to have a look at the current part of instrument, '
                'type ".." to go back'
            )
            print()
            if delimiter:
                try:
                    payload = input(f'{message}: ').split(delimiter)
                except KeyboardInterrupt:
                    print()
                    print('(×_×)')
                    exit(0)
                if payload[0] == '.':
                    self.__ls()
                    input('Press Enter to continue')
                    clear()
                elif payload[0] == '..':
                    print('Going back, no changes applied')
                    return None
                else:
                    return payload
            else:
                try:
                    payload = input(f'{message}: ')
                except KeyboardInterrupt:
                    print()
                    print('(×_×)')
                    exit(0)
                if payload == '.':
                    self.__ls()
                    input('Press Enter to continue')
                    clear()
                elif payload == '..':
                    print('Going back, no changes applied')
                    return None
                else:
                    return payload

    def set_value(
            self,
            path: list,
            old_value,
            new_input,
            field_type: type,
            opts_list=None,
            action=None,
            is_new=False
        ):
        # OMG, that's boring!
        # field types:
        # bool        - generally True or False accepted in various human-readable forms
        # int, float  - self explaining
        # str         - may take any alphanumeric string
        # list        - if content is not empty, items are dicts of defined structure
        #               otherwise items are str type
        # select      - may take one preselected value defined in config
        # multiselect - may take more than one preselected values defined in config
        # sdb_select  - list of acccepted values is fetched from sdb, e.g.
        #               exchangeId, currency, scheduleId, etc.
        # sdb_date    - takes ISO-formatted date or datetime string and transforms it
        #               to the dict: {
        #                   year: int,
        #                   month: int,
        #                   day: int,
        #                   time: str  # optional
        #               }
        #               
        new_value = None
        key = path[-1]
        header = f"{self.__navigate()}\n{key}: {old_value}"
        message = f'Set the new value for {key}, leave empty to delete/inherit'

        
        def set_simple(old_value=None, new_input: str = None, **kwargs):
            if new_input:
                if new_input == '/rm':
                    return None
                try:
                    new_value = kwargs.get('field_type')(new_input)
                    return new_value
                except ValueError:
                    input(f'{field_type} is expected')
                    new_value = None
            else:
                new_value = None
            while new_value is None:
                new_input = self.__general_input(message=message, header=header, delimiter=None)
                if new_input is None:
                    return old_value
                elif new_input == '/rm':
                    return None
                else:
                    try:
                        new_value = field_type(new_input)
                    except ValueError:
                        input(f'{field_type} is expected')
            return new_value
        
        def set_bool(old_value: bool = None, new_input: str = None, **kwargs):

            if new_input in ['yes', 'Y', 'true', 'True', 'enable', 'enabled', 'on', '1']:
                return True
            elif new_input in ['no', 'N', 'false', 'False', 'disable', 'disabled', 'off', '0']:
                return False
            elif new_input == '/rm':
                return None
            elif new_input is not None:
                return old_value
            elif is_new:
                choices = ['False (Disable)', 'True (Enable)', '..']
                selected = pick_from_list_tm(choices, message=message)
                if selected is None or choices[selected] == '..':
                    print('Going back, no changes applied')
                    return None
                return bool(selected)
            else:
                if old_value is True:
                    return False
                elif old_value is False or old_value is None:
                    return True
                else:
                    return False

        def set_select(old_value: str = None, new_input: str = None, **kwargs):
            options = kwargs.get('options_list')
            if not isinstance(options[0], tuple):
                options = [(x, x) for x in options]
            options = options + [('remove/inherit', 'remove/inherit'), ('..', '..')]
            new_value = None
            while new_value is None:
                selected = pick_from_list_tm(options, key, specify=new_input)
                if selected is None:
                    return old_value
                selected_choice = options[selected]
                if selected_choice[1] == '..':
                    return old_value
                elif selected_choice[1] == 'remove/inherit':
                    return None
                else:
                    return selected_choice[1]

        def set_multiselect(old_value: list = None, new_input: str = None, **kwargs):
            options = kwargs.get('options_list')
            if old_value:
                new_value = deepcopy(old_value)
            else:
                new_value = list() 
            for ni in new_input.split(','):
                choices = [x for x in options if x not in new_value]
                choices += ['remove/inherit', '..', 'Done']
                selected = pick_from_list_tm(
                    choices,
                    key, 
                    '\n· '.join([f'{key}:'] + new_value),
                    specify=ni)
                if selected is None:
                    return old_value
                elif choices[selected] == 'remove/inherit':
                    new_value = list()
                elif choices[selected] == '..':
                    return old_value
                elif choices[selected] == 'Done':
                    break
                else:
                    new_value.append(choices[selected])
            while choices[selected] != 'Done':
                choices = [x for x in options if x not in new_value]
                choices += ['remove/inherit', '..', 'Done']
                selected = pick_from_list_tm(choices, key, '\n· '.join([f'{key}:'] + new_value))
                if selected is None:
                    return old_value
                elif choices[selected] == 'remove/inherit':
                    new_value = []
                elif choices[selected] == '..':
                    return old_value
                elif choices[selected] == 'Done':
                    pass
                else:
                    new_value.append(choices[selected])
            print(f"{key}: {new_value}")
            return new_value

        def set_sdb_date(old_value: dict = None, new_input: str = None, **kwargs):
            new_date = None
            new_time = None
            if new_input:
                if new_input == '/rm':
                    return None
                try:
                    new_datetime = new_input.split('T')
                    new_date = dt.date.fromisoformat(new_datetime[0])
                except ValueError:
                    new_datetime = new_input.split(' ')
                    try:
                        new_date = dt.date.fromisoformat(new_datetime[0])
                    except ValueError:
                        input(f'{new_input} is wrong date or datetime format, try again')
                if len(new_datetime) > 1:
                    try:
                        dt.time.fromisoformat(new_datetime[1])
                        new_time = new_datetime[1]
                    except ValueError:
                        input(f'{new_time} is wrong time format, try again')
            while new_date is None:
                header = 'Type date and time as: YYYY-MM-DDThh:mm:ss,\ntime is optional and relative to the timezone (not UTC)'
                new_datetime = self.__general_input(message=f'Set the new value for {key}', header=header, delimiter='T')
                if new_datetime is None or not new_datetime[0]:
                    return old_value
                elif new_datetime[0] == '/rm':
                    return None
                try:
                    if len(new_datetime):
                        new_date = dt.date.fromisoformat(new_datetime[0])
                    if len(new_datetime) > 1:
                        new_time = new_datetime[1]
                    else:
                        new_time = None
                    if new_time:
                        dt.time.fromisoformat(new_time) #nothing to write here, just test if input time is right format
                except ValueError:
                    input(f'{new_input} is wrong format, try again')
            new_value = {
                'year': new_date.year,
                'month': new_date.month,
                'day': new_date.day
            }
            if new_time:
                new_value.update({'time': new_time})
            return new_value

        def set_list(old_value: list = None, new_input: str = None, **kwargs):
            header = str()
            if old_value:
                new_value = old_value.copy()
            else:
                new_value = list()
            if new_input:
                if new_input == '/rm':
                    return None
                new_value += new_input.split(',')
                return new_value
            while True:
                if not new_value and old_value:
                    header = 'Current list:\n' + '\n· '.join(old_value) + '\n'
                elif not new_value:
                    header = 'Current list is empty\n'
                else:
                    header = f'{key}:\n· ' + '\n· '.join(new_value) + '\n'
                header += '\nNew entries will be appended to the current list, type "/rm" to clear it\n'
                header += 'Type entries in one line divided by "," or one by one, press Enter when finished\n'
                new_list = self.__general_input(message='Type entries', header=header, delimiter=',')
                if new_list is None:
                    return old_value
                elif not new_list[0]:
                    return new_value
                elif new_list[0] == '/rm':
                    new_value = list()
                    new_list = list()
                else:
                    new_value += new_list

        def set_template(old_value: str = None, new_input: str = None, **kwargs):
            compiled_template = ''
            test_string = new_input if new_input else old_value
            if not test_string:
                test_string = input('Type template: ')
            while True:
                message = ''
                compiled_template = self.sdbadds.lua_compile(self.modify_copy, test_string, compiled=True)
                
                if compiled_template and new_input:
                    return {
                        '$template': test_string
                    }
                elif compiled_template:
                    message = f'Current template compilation: {compiled_template}' + '\n'
                else:
                    message = f'Current template is not valid' + '\n'
                
                message += 'Type "Done" when done, type ".." to leave unchanged'
                input_string = self.__general_input(
                    message, header=test_string, delimiter=None
                )
                if input_string is None:
                    return {
                        '$template': old_value
                    }
                elif input_string == 'Done':
                    return {
                        '$template': test_string
                    }
                else:
                    test_string = input_string
        
        def set_path(**kwargs):
            choices = []
            target = copy(self.__get_part(['path']))
            instr_id = target[-1]
            choices = [self.sdbadds.uuid_to_name(f) for f in target]
            choices.append('.')
            choices.append('..')
            if self.input_list:
                pick = self.input_list.pop(0) if self.input_list else None
            else:
                pick = None
            fld_selected = pick_from_list_tm(choices, 'folders', specify=pick)
            if fld_selected is None:
                return target
            if choices[fld_selected] == '.':
                self.__ls()
                input('Press Enter to continue')
            elif choices[fld_selected] == '..':
                print('Going back, no changes applied')
                return target
            elif isinstance(choices[fld_selected], tuple):
                target = target[:fld_selected+1]
                go_deeper = True
                while go_deeper:
                    pick = None
                    go_deeper = False
                    if self.input_list:
                        pick = self.input_list.pop(0) if self.input_list else None
                    neighbors = [
                        (x['name'], x['_id']) for x
                        in asyncio.run(self.sdb.get_heirs(
                            target[-1],
                            fields=['name']
                        )) 
                        if x['isAbstract'] == True
                    ]
                    neighbors.append('..')
                    neighbors.append('Done')
                    neighbors.append('Reset to initial path')
                    message = self.sdbadds.show_path(target)
                    opt_selected = pick_from_list_tm(neighbors,
                                                option_name='folders',
                                                message=message,
                                                specify=pick)
                    if opt_selected is None:
                        return self.__get_part(['path'])
                    elif neighbors[opt_selected] == 'Done':
                        target.append(instr_id)
                        # if update_size \
                        if self.modify_copy.get('isAbstract')\
                            and target != self.__get_part(['path']):

                            children = asyncio.run(self.sdb.get_heirs(self.modify_copy['_id'], recursive=True))
                            if len(children) and input(
                                    f'Should the children (total: {len(children)} instruments) '
                                    f'paths also be fixed? (N/y): '
                                ) == 'y':
                                children = asyncio.run(
                                    self.sdb.get_heirs(
                                        self.modify_copy['_id'], full=True, recursive=True
                                    )
                                )
                                
                                for c in children:
                                    old_folder_index = c['path'].index(instr_id)
                                    c['path'] = target + c['path'][old_folder_index + 1:]
                                    self.final_update_list.append(c)
                    elif neighbors[opt_selected] == 'Reset to initial path':
                        target = deepcopy(self.__get_part(['path']))
                    elif neighbors[opt_selected] == '..':
                        target = target[:-1]
                        go_deeper = True
                    elif isinstance(neighbors[opt_selected], tuple):
                        target.append(neighbors[opt_selected][1])
                        go_deeper = True
                return target
        
        def set_underlying(old_value: str = None, new_input: str = None, **kwargs):
            message = f"Choose underlying for {self.exanteid}:"
            while True:
                new_underlying_name, new_underlying_id = self.sdbadds.browse_folders(
                    [],
                    message=message,
                    allowed=['CFD', 'FUTURE', 'INDEX', 'STOCK']
                )
                if not new_underlying_name:
                    return old_value

                found_in_tree = self.sdbadds.tree_df.loc[self.sdbadds.tree_df['_id'] == new_underlying_id]
                if found_in_tree.empty:
                    input(f"Can't find {new_underlying_id} in tree :(")
                    continue
                found_in_tree = found_in_tree.iloc[0]
                if found_in_tree['isAbstract'] or self.sdbadds.isexpired(found_in_tree.to_dict()) or found_in_tree['isTrading'] is False:
                    input('Underlying should be a tradable, not expired instrument! try again')
                    continue
                break
            underlying_symbol_id = found_in_tree.get('symbolId', self.sdbadds.compile_symbol_id(found_in_tree['_id']))
            if underlying_symbol_id:
                return {
                    'type': 'symbolId',
                    'id': underlying_symbol_id
                }
            else:
                return old_value
        
        actions = {
            'set_simple': set_simple,
            'set_bool': set_bool,
            'set_select': set_select,
            'set_multiselect': set_multiselect,
            'set_sdb_date': set_sdb_date,
            'set_list': set_list,
            'set_template': set_template,
            'set_path': set_path,
            'set_underlying': set_underlying,
        }

        if action in actions:
            new_value = actions[action](
                old_value=old_value,
                new_input=new_input,
                options_list=opts_list,
                field_type=field_type
            )
        else:
            return old_value
        if new_value == '' or new_value is None:
            self.inherited_value_trigger = True
            self.check_inheritance(path)
        change = {
            '/'.join([str(x) for x in path]): {
                'old': old_value,
                'new': new_value
            }
        }
        self.logger.debug(pformat(change))
        self.edit_history.append(change)
        return new_value

    def modify_part(
            self,
            path: list = None,
            recurrency: int = None,
            parent_target=None,
            is_new: bool = False
        ) -> bool:
        # init part
        action = None
        confirm = True
        popped = None
        self.inherited_value_trigger = False
        path = path if path else self.path
        # reference is the part we want to return if we cancel all changes
        # · if the part already exists in instrument we keep it as reference
        # · if we build new dict with nested fields we want to return this newly created
        #   dict in case if we cancel to set nested field but we don't want to write this dict into
        #   instrument yet because we could cancel it creation as well, that's why our reference
        #   is a temporary dict
        reference = deepcopy(self.__get_part(path=path)) if not parent_target else parent_target[path[-1]]
        # as always parent is nearest named field
        # (meaning uuid names as provider uuid or order number in list doesn't count)
        parent = next((
            x for x in reversed(path)
            if isinstance(x, str)
            and x not in [
                y[1] for y
                in (
                    asyncio.run(
                        self.sdbadds.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value)
                    )
                    + asyncio.run(
                        self.sdbadds.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value)
                    )
                )
            ]
        ), None)
        recurrency = recurrency if recurrency else 0
        field_attributes = self.__get_properties(path=path)
        if not field_attributes:
            input('Unsuccessful schema lookup, value not changed. Press Enter')
            return False
        new_input = self.input_list.pop(0) if self.input_list else None
        field_title = field_attributes.get('title', '').lower()
        field_type = field_attributes.get('type')
        opts_list = field_attributes.get('opts_list')
        old_value = self.__get_part(path)
        # end of init part
        
        # decide what to do
        # setting action means we will go with that to set_value

        # let's check field_title first
        if field_title == 'path': # apparently it's not Capitalized
            action = 'set_path'
        elif field_title == 'sdbdate':
            action = 'set_sdb_date'
        elif field_title == '$template':
            if not old_value.get('$template'):
                old_value.update({
                    '$template': ''
                })
            old_value = old_value['$template']
            action = 'set_template'
        elif field_title == 'underlyingid':
            action = 'set_underlying'
        elif field_title in ['accounts', 'gateways']:
            self.move_around(self.__get_part(path), path=path) # add history
            confirm = False

        # now field_type
        elif field_type == 'string' and opts_list:
            action = 'set_select'
            confirm = False
        elif field_type in ['string', 'number', 'integer']:
            action = 'set_simple'
            confirm = False
        elif field_type == 'boolean':
            action = 'set_bool'
            confirm = False

        # the rest possible are array and object
        elif field_type == 'array' and field_attributes['items'].get('type') and opts_list:
            action = 'set_multiselect'
        elif field_type == 'array' and field_attributes['items'].get('type'):
            action = 'set_list'
        elif field_type == 'array':
            pass
            # arrays of comlex items
            # should be written as separate methods to ensure safe modifications
            # for now everything in list below could be set by existing methods
            # in slightly less safe manner

            # all complex type arrays by the way:
            # Accounts
            # FatalRejects
            # Gateways
            # InstantMarkup
            # Legs
            # StrikePrice
            # SyntheticSources
        elif field_type == 'object':
            self.edit_dict(self.__get_part(path), path=path, recurrency=recurrency+1)
        else:
            print(f"Can't recognize the {path[-1]} field type, sorry")
            return False

        if action:
            self.__get_part(path[:-1])[path[-1]] = self.set_value(
                path,
                old_value,
                new_input,
                field_type=type_mapping[field_type],
                opts_list=opts_list,
                action=action,
                is_new=is_new
            )
            if self.__get_part(path) is None:
                popped = self.__get_part(path[:-1]).pop(path[-1])
        while True:
            if self.inherited_value_trigger:
                message = (
                    f"{popped} value is set to None, "
                    f"following value will be inherited: {self.check_inheritance(path)}"
                )
            elif not recurrency == 0 or not confirm:
                return True
            if action:
                message = '\n'.join([
                    f"{d[0]}{d[1]}" for d 
                    in self.sdbadds.fancy_dict(
                        self.__get_part(path[:-1]),
                        parent=parent,
                        exclude=self.always_exclude,
                        recursive=50
                    )
                ])
            else:
                message = '\n'.join([
                    f"{d[0]}{d[1]}" for d 
                    in self.sdbadds.fancy_dict(
                        self.__get_part(path[:-1]),
                        parent=parent,
                        exclude=self.always_exclude,
                        recursive=50
                    )
                ])
            message += '\nLooks good?'
            actions = ['Save', '.', '..']
            pick = self.input_list.pop(0) if self.input_list else None
            to_do = pick_from_list_tm(actions, 'actions', message=message, specify=pick)
            if to_do is None:
                if path:
                    self.__get_part(path[:-1])[path[-1]] = reference
                else:
                    self.modify_copy = reference
                return False
            elif actions[to_do] == '.':
                self.__ls()
                input('Press Enter to continue')
                self.input_list = []
            elif actions[to_do] == '..':
                if path:
                    self.__get_part(path[:-1])[path[-1]] = reference
                else:
                    self.modify_copy = reference
                self.edit_history.pop(-1)
                return False
            elif actions[to_do] == 'Save':
                return True

    def move_around(self, target: list, path: list = None) -> Union[bool, None]:
        actions = [
            '⮝ Move up',
            '⮟ Move down',
            '⭙ Delete from list',
            '◯ Release',
            '..'
        ]
        parent = path[-1]
        def preview(action):
            act_sign = action[:2]
            members = [
                x[1] for x
                in self.sdbadds.fancy_dict(target, parent=parent, recursive=0) 
                if x[2] is not None
            ]
            members[selected] = act_sign + members[selected]
            return '\n'.join(members)

        reference = deepcopy(target)
        initial_message = f"Select one of {parent} to change order or remove"
        while True:
            members = ['Add new', 'Save', '..']
            members += [
                x[1] for x
                in self.sdbadds.fancy_dict(target, parent=parent, recursive=0)
                if x[2] is not None
            ]
            selected = pick_from_list_tm(
                members,
                option_name=parent,
                message=initial_message
            )
            if selected is None:
                continue
            if selected == 0:
                if parent == 'accounts':
                    added = self.add_new_account(target)
                elif parent == 'gateways':
                    added = self.add_new_gateway(target)
                continue
            elif selected == 1:
                return True
            elif selected == 2:
                target = reference
                return False
            selected -= 3
            selected_part = target[selected]
            action_message = f"Moving {members[selected]}:"
            pick = 0
            while selected is not None:
                pick = pick_from_list_tm(
                    actions,
                    option_name=parent,
                    message=action_message,
                    preview=preview,
                    cursor_index=pick)
                if pick == 0: # up
                    if selected > 0:
                        target.insert(selected-1, target.pop(selected))
                        selected -= 1
                elif pick == 1: # down
                    if selected < len(target)-1:
                        target.insert(selected+1, target.pop(selected))
                        selected +=1
                elif pick == 2: # delete
                    target.pop(selected)
                    selected = None
                elif pick == 3: # release
                    selected = None
                elif pick == 4: # go back
                    target = reference
                    return None
            old_order = [
                x[1] for x
                in self.sdbadds.fancy_dict(reference, parent=parent, recursive=0)
                if x[2] is not None
            ]
            new_order = [
                x[1] for x
                in self.sdbadds.fancy_dict(target, parent=parent, recursive=0)
                if x[2] is not None
            ]
            change = {
                '/'.join([str(x) for x in path]): {
                    'old': old_order,
                    'new': new_order
                }
            }
            self.logger.debug(pformat(change))
            self.edit_history.append(change)

    def edit_dict(self, target: dict = None, path: list = None, recurrency: int = None) -> bool:
        if recurrency is None:
            recurrency = 0
        if path is None:
            path = self.path
        if target is None:
            target = self.__get_part(path)
        parent = next((
            x for x in reversed(path)
            if isinstance(x, str)
            and x not in [
                y[1] for y
                in (
                    asyncio.run(
                        self.sdbadds.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value)
                    )
                    + asyncio.run(
                        self.sdbadds.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value)
                    )
                )
            ]
        ), None)
        reference = deepcopy(target)
        pick = self.input_list.pop(0) if self.input_list else None
        fields = list(target.keys())
        fields.insert(0, '..')
        fields.insert(0, 'Remove')
        fields.insert(0, 'Add new')
        selected = pick_from_list_tm(
            fields,
            'items',
            message=f'Select items of {parent}',
            specify=pick
        )
        if selected is None:
            print('Going back, no changes applied')
            target = reference
            return False
        elif fields[selected] == '..':
            print('Going back, no changes applied')
            target = reference
            return False
        elif fields[selected] == 'Add new':
            self.add_new_dict(target, path, recurrency=recurrency+1)
        elif fields[selected] == 'Remove':
            self.__get_part(path[:-1]).pop(path[-1])
        else:
            self.modify_part(path+[fields[selected]], recurrency=recurrency+1)
        change = {
            '/'.join([str(x) for x in path]): {
                'old': reference,
                'new': target
            }
        }
        self.logger.debug(pformat(change))
        self.edit_history.append(change)
        return True

    def add_new_gateway(self, target: list = None) -> bool:
        if not target:
            target = self.__get_part(['feeds', 'gateways'])
        present = [x['gatewayId'] for x in target]
        choices = [
            x for x
            in asyncio.run(
                self.sdbadds.get_list_from_sdb(SdbLists.GATEWAYS.value, id_only=False)
            )
            if x[1]['gatewayId'] not in present
        ]
        choices.append('.')
        choices.append('..')
        pick = self.input_list.pop(0) if self.input_list else None
        gw_selected = pick_from_list_tm(
            choices,
            'gateways',
            message='select gateway',
            specify=pick
        )
        if gw_selected is None:
            return False
        elif choices[gw_selected] == '.':
            self.__ls()
            input('Press Enter to continue')
        elif choices[gw_selected] == '..':
            print('Going back, no changes applied')
            return False
        else:
            new_gw = deepcopy(choices[gw_selected][1])
            new_gw['gateway'].update({
                'enabled': True,
                'allowFallback': True
            })
            target.insert(0, new_gw)
            change = {
                'feeds/gateways': {
                    'old': None,
                    'new': new_gw
                }
            }
            self.logger.debug(pformat(change))
            self.edit_history.append(change)

            return True

    def add_new_account(self, target: list = None) -> bool:
        if not target:
            target = self.__get_part(['brokers', 'accounts'])
        present = [x['accountId'] for x in target]
        accs_choices = [
            x for x
            in asyncio.run(
                self.sdbadds.get_list_from_sdb(SdbLists.ACCOUNTS.value, id_only=False)
            )
            if x[1]['accountId'] not in present
        ]
        accs_choices.append('.')
        accs_choices.append('..')
        pick = self.input_list.pop(0) if self.input_list else None
        accs_selected = pick_from_list_tm(
            accs_choices,
            'accounts',
            message='select account',
            specify=pick
        )
        if accs_selected is None:
            return False
        elif accs_choices[accs_selected] == '.':
            self.__ls()
            input('Press Enter to continue')
        elif accs_choices[accs_selected] == '..':
            print('Going back, no changes applied')
            return False
        else:
            account_name, new_account = deepcopy(accs_choices[accs_selected])
            execution_schemes = [
                (ex['name'], ex['_id']) for ex
                in asyncio.run(self.sdbadds.load_execution_to_route())
                if new_account['accountId'] in [y['_id'] for y in ex['routes']]
            ]
            if len(execution_schemes) == 1:
                execution_scheme = execution_schemes[0]
            elif len(execution_schemes):
                selected_ex = pick_from_list_tm(
                    execution_schemes,
                    'execution schemes',
                    clear_screen=False
                )
                execution_scheme = execution_schemes[selected_ex]
            else:
                self.logger.warning(f"Cannot find execution scheme for {account_name}")
                execution_scheme = None
            new_account['account'].update({
                'enabled': True,
                'allowFallback': True
            })
            if execution_scheme:
                new_account['account'].update({
                    'executionSchemeId': execution_scheme[1]
                })
            target.insert(0, new_account)
            change = {
                'brokers/accounts': {
                    'old': None,
                    'new': new_account
                }
            }
            self.logger.debug(pformat(change))
            self.edit_history.append(change)

            return True

    def add_new_dict(self, target: dict, path: list = None, recurrency: int = None) -> bool:
        if path is None:
            path = self.path
        if recurrency is None:
            recurrency = 0
        done = False
        parent = next((
            x for x in reversed(path)
            if isinstance(x, str)
            and x not in [
                y[1] for y
                in (
                    asyncio.run(
                        self.sdbadds.get_list_from_sdb(SdbLists.BROKER_PROVIDERS.value)
                    )
                    + asyncio.run(
                        self.sdbadds.get_list_from_sdb(SdbLists.FEED_PROVIDERS.value)
                    )
                )
            ]
        ), None)
        while not done:
            properties = self.__get_properties(path=path)
            fields_tree = properties.get('properties', {})
            if fields_tree:
                available = [
                    x for x
                    in sorted(fields_tree.keys())
                    if x not in target.keys()
                ]
            else:
                available = properties.get('opts_list', [])

            available.append('.')
            available.append('..')
            pick = self.input_list.pop(0) if self.input_list else None
            if parent:
                message = f'Add new item to the {parent}'
            else:
                message = f'Add new item'
            selected = pick_from_list_tm(
                available, 'available items', message=message, specify=pick
            )
            if selected is None or available[selected] == '..':
                print('Going back, no changes applied')
                return False
            available_selected = available[selected]
            if available_selected == '.':
                self.__ls()
                input('Press Enter to continue')
                continue
            if isinstance(available_selected, tuple):
                available_selected = available_selected[1]
            props = self.__get_properties(path + [available_selected])

            target.update({
                available_selected: type_mapping[props['type']]()
            })
            added = self.modify_part(
                path=path+[available_selected],
                recurrency=recurrency + 1, 
                parent_target=target,
                is_new=True
            )
            change = {
                '/'.join([str(x) for x in path]): {
                    'old': None,
                    'new': available_selected
                }
            }
            self.logger.debug(pformat(change))
            self.edit_history.append(change)

            # if target[available_selected] is None:
            #     target.pop(available_selected)
            return added
    
    def edit_payment_dates(self, target):
        done = False
        while not done:
            select_date = self.input_list.pop(0) if self.input_list else None
            choices = sorted(list(target.keys()))
            choices += ['All one by one',
                        'Multiply all by a number',
                        'Add new',
                        'Remove',
                        'Done',
                        '.',
                        '..']
            selected = pick_from_list_tm(
                choices, 
                'dates', 
                message='Select payment date to edit', 
                specify=select_date
            )
            if selected is None:
                continue
            elif choices[selected] == '.':
                self.__ls(recurrency=2)
                input('Press Enter to continue')
            elif choices[selected] == '..':
                return target
            elif choices[selected] == 'Done':
                done = True
            elif choices[selected] == 'Multiply all by a number':
                try:
                    multiplier = float(self.input_list.pop(0)) if self.input_list else None
                except ValueError:
                    print('Wrong multiplier should be a number')
                    self.input_list = []
                    multiplier = None
                while multiplier is None:
                    message = 'Type number to multiply coupons and redemption'
                    new_input = self.__general_input(message=message, delimiter=None)
                    if new_input is None:
                        return target
                    try:
                        multiplier = float(new_input)
                    except ValueError:
                        print('Wrong multiplier, should be a number')
                        multiplier = None
                for pdate in target:
                    target[pdate][0] = str(float(target[pdate][0]) * multiplier)
                    target[pdate][1] = str(float(target[pdate][1]) * multiplier)
                done = True
            elif choices[selected] == 'Add new':
                done_add = False
                new_dates = list()
                data = self.input_list.pop(0).split(',') if self.input_list else list()
                if data and data[-1] =='Done':
                    done_add = True
                    data = data[:-1]
                if not len(data)%3:
                    for i in range(int(len(data)%3)):
                        try:
                            dt.date.fromisoformat(data[3 * i])
                            float(data[i * 3 + 1])
                            float(data[i * 3 + 2])
                            new_dates.append({
                                data[3 * i]: [
                                    data[i * 3 + 1],
                                    data[i * 3 + 2]
                                ]
                            })
                        except ValueError:
                            print(f'{data[i * 3 : i * 3 + 3]} is wrong format, not added')
                while not done_add:
                    header = 'Type new payment date (YYYY-MM-DD), coupon and redemption, divided by ","'
                    new_input = self.__general_input(message='New payment date', header=header, delimiter=',')
                    if new_input is None:
                        return target
                    elif new_input[-1] == 'Done':
                        done_add = True
                        new_input.pop(-1)
                    if not len(new_input)%3:
                        for i in range(int(len(new_input)%3)):
                            try:
                                dt.date.fromisoformat(new_input[3 * i])
                                float(new_input[i * 3 + 1])
                                float(new_input[i * 3 + 2])
                                new_dates.append({
                                    new_input[3 * i]: [
                                        new_input[i * 3 + 1],
                                        new_input[i * 3 + 2]
                                    ]
                                })
                            except ValueError:
                                print(f'{new_input[i * 3 : i * 3 + 3]} is wrong format, not added')
            else:
                if choices[selected] == 'All one by one':
                    dates_to_edit = list(target.keys())
                else:
                    dates_to_edit = [choices[selected]]
                data = self.input_list.pop(0).split(',') if self.input_list else None
                for pdate in dates_to_edit:
                    coupon_value = None
                    redemption = None
                    if data and not data[0]:
                            new_date = pdate
                    elif data:
                        try:
                            dt.date.fromisoformat(data[0])
                            new_date = data.pop(0)
                        except ValueError:
                            data = None
                            new_date = None
                    while not new_date:
                        header = 'Press Enter if current value is ok, otherwise type the right one'
                        new_input = self.__general_input(message=f'Current date {pdate}', header=header, delimiter=None)
                        if new_input is None:
                            return target
                        elif not new_input:
                            new_date = pdate
                        else:
                            try:
                                dt.date.fromisoformat(new_input)
                            except ValueError:
                                print(f'{new_date} is wrong format, try again')
                                new_date = None
                    while coupon_value is None and redemption is None:
                        try:
                            if data:
                                coupon_value = data.pop(0)
                            if data:
                                redemption = data.pop(0)
                            if coupon_value is None:
                                coupon_value = input(f'Type coupon value (current {target.get(pdate, [None, None])[0]}): ')
                            if redemption is None:
                                redemption = input(f'Type redemption value (current {target.get(pdate, [None, None])[1]}): ')
                            if not coupon_value:
                                coupon_value = target.get(pdate)[0]
                            if not redemption:
                                redemption = target.get(pdate)[1]
                            coupon_value = str(float(coupon_value))
                            redemption = str(float(redemption))
                        except ValueError:
                            print('Wrong value, should be a number (float)')
                            coupon_value = None
                            redemption = None
                        except IndexError:
                            print('No old values have been found, fill them manually')
                            coupon_value = None
                            redemption = None
                        except KeyboardInterrupt:
                            print()
                            print('(×_×)')
                            exit(0)
                    target.pop(pdate)
                    target.update({new_date: [coupon_value, redemption]})
                if choices[selected] == 'All one by one':
                    done = True
        return target

    def check_inheritance(self, path: list = None):
        if path is None:
            path = self.path
        current_part = deepcopy(self.modify_copy)
        try:
            legacy = asyncio.run(self.sdbadds.build_inheritance(self.instrument))
            for p in path:
                current_part = current_part[p]
                if isinstance(p, str):
                    legacy = legacy.get(p)
                    if legacy is None:
                        break
                elif isinstance(p, int):
                    entry_type, entry_id = next(
                        x for x
                        in current_part.items() 
                        if isinstance(x[1], str)
                    )
                    legacy = next((
                        x for x 
                        in legacy 
                        if x[entry_type] == entry_id
                    ), None)
                    if not legacy:
                        break
        except Exception as e:
            self.logger.warning(f"{e.__class__.__name__}: {e}. Can't get inheritable value")
            legacy = None
        return legacy

    def print_help(self, section) -> str:
        message = str()
        if section == 'init':
            message = '''
        · Type nested keys one by one or all at once divided by ";"
        · Feel free to enter any piece of long name,
            if it cannot be determined unambiguously
            you'll be prompted to select suitable option
        · Type "Done" to update the instrument
        · Press enter to see available options if any
        · Type "." to have a look at the current part
        · Type ".." to go up or leave part unchanged if in editing mode
            '''
        # elif section == 'general':
        #     print('''
        # · Press Enter to see available options
        # · Type "help" to show this message again
        # · Type "." to have a look at the current part
        # · Type ".." to go up or leave part unchanged if in editing mode
        #     ''')
        elif section == 'list_rr':
            message = '''
        · Members of list could be called by order number or name
            (partial names also acepted)
        · Call at least two members separated by "," to rearrange the list,
            all mentioned positive entries will be moved to the top
            along with the given input order,
            use negative numbers to move entry to the bottom
            (i.e. to move, say, 3rd entry to the top type "3,1"
            to move 2nd entry to the bottom and 4th to the top
            type "-2,4" or "4,-2")
        · Call one member to remove it
        · First item in list is 1
            '''
        elif section == 'list_new':
            message = '''
        · Type order number to put new entry into place
            use negative number to count order from the bottom
            (i.e. to put new value to the very end of the list
            type "-1", to put new value to the 3rd place type "3")
        · First item in list is 1
            '''
        elif section == 'set_value':
            message = '''
        · Type value by hand for float, int, or str types
        · You'll be prompted to select from list for limited choice values
        · Type "/rm" to delete/inherit value
        · Type "." to have a look at the current part
        · Type ".." in any place to leave the value unchanged
            '''
        return message
    
    def edit_instrument(self, highlight: dict = None):
        # general rules to make controls:
        # · '..' is always go back without saving or go one level up in dict
        # · '.'  is always to have a look at where we are, doesn't change anything
        # · navigation through the dict:
        #   ° in dict entity access is made by simply typing field name or part of field name,
        #     that allows to distinguish it from other names
        #   ° in lists you always can access required item by index (first item is 1, not 0),
        #     additionally, in some lists like gateways or accounts, you can access the item by its name,
        #     e.g. BLOOMBERG: PROD: DEFAULT
        #   ° if the field name is uuid (like in providerOverrides) you can access it by its name,
        #     e.g. BLOOMBERG
        # · all commands which are not field access Are Capitalized, e.g. 'Add new', 'Edit', 'Done'
        #
        # · More about navigation and input ambiguity:
        # Suppose you have instrument AAPL.NASDAQ. If you type 'Sche' there are
        # two options that fit to this input: quoteMonitorScheduleId and applyExecutionScheme.
        # Editor cannot decide which one to choose, so menu with options will appear.
        # If your intention was to select applyExecutionScheme, input 'Schem' would be enough
        # to make unambiguous choice.
        # This will also happen when you feed all arguments in one line, so be precise in your
        # abbreviations otherwise editor will pause in front of such uncertainty
        self.highlight = highlight if highlight else {}
        self.path = []
        self.edit_history = []
        if self.input_list:
            print(f"processing following input line: {self.input_list}")
        inline = False
        # main cycle
        # input cycle is switching by two modes:
        # either select from menu or typing commands in one line
        while True:
            input_selected = None
            # find where we are
            nav_string = self.__navigate()
            part_to_edit = self.__get_part()
            part_schema = self.__get_properties()
            parent = next((x for x in reversed(self.path) if isinstance(x, str)), None)
            while input_selected is None:
                # make address line
                # get input
                # if we already have a list of inputs take first
                input_entry = self.input_list.pop(0) if self.input_list else None
                # make list of choices at current place
                current_part_content = [
                    ['', 'Done', 'Done'],
                    ['', '.', '.']
                ]
                indent = []
                human_path = self.__navigate(get_list=True)
                for i in human_path:
                    current_part_content.append([''.join(indent), i, i])
                    indent.append('│')
                cursor = len(current_part_content)
                current_part_content += [x for x
                    in self.sdbadds.fancy_dict(
                        part_to_edit,
                        recursive=0,
                        parent=parent,
                        exclude=self.always_exclude,
                        depth=indent[1:]
                    )
                    if x[2] is not None
                ]
                # if we already have input_entry, check if it fits unambiguously to one of choices,
                # immediately select if so, otherwise show menu
                selected = None
                if input_entry or not inline:
                    if not highlight:
                        message = nav_string
                    else:
                        message = "Following fields are invalid:\n"
                        message += '\n'.join([
                            f"{loc}: {msg}" for loc, msg
                            in highlight.items()
                        ])
                    selected = pick_from_list_tm(
                        [f"{x[0]}{x[1]}" for x in current_part_content],
                        'options',
                        message=message,
                        specify=input_entry,
                        cursor_index=cursor
                    )
                # if pressed Esc in menu then prompt to type the line of commands manually
                if selected is None:
                    inline = True
                else:
                    input_selected = current_part_content[selected][2]
                    continue
                if inline:
                    self.input_list = self.__general_input(
                        message=f'{nav_string}',
                        header=self.print_help('init')
                    )
                    if self.input_list is None:
                        input_selected = '..'
                    
            if isinstance(input_selected, (list, tuple)):
                input_selected = input_selected[1]

            # now we have the selected option, let's decide what to do
            if input_selected == 'Done':
                # to do: validate with schema
                return self.modify_copy     # finish instrument edit and save
            elif input_selected == '.':    # general key to go back or one level up
                self.__ls()
                input('Press Enter to continue')
            elif input_selected in human_path[:-1]:
                self.path = self.path[:human_path.index(input_selected)]
                if len(self.path) > 1 and self.path[-2] == 'gateways':
                    self.path.append('gateway')
                if len(self.path) > 1 and self.path[-2] == 'accounts':
                    self.path.append('account')
            elif input_selected == human_path[-1]:
                self.modify_part()
            elif isinstance(input_selected, int):               # access to item in list
                self.path.append(input_selected)
                if len(self.path) > 1 and self.path[-2] == 'gateways':
                    self.path.append('gateway')
                if len(self.path) > 1 and self.path[-2] == 'accounts':
                    self.path.append('account')
                
            elif input_selected in part_to_edit \
                and isinstance(part_to_edit[input_selected], (list, dict)):
                self.path.append(input_selected)
            elif input_selected in part_to_edit:
                self.modify_part(path=self.path+[input_selected])                
            elif not part_schema.get('properties')\
                and part_schema.get('type'):  # some simple type field like bool or str
                self.modify_part(path=self.path+[input_selected])
            fixed = next((
                list(x.keys())[0] for x
                in self.edit_history
                if list(x.keys())[0] in self.highlight
            ), None)
            if fixed:
                self.highlight.pop(fixed)

    def verify(self, instrument):
        preserve = [
            'gatewayId',
            'accountId',
            'providerId',
            'path',
            'executionSchemeId',
            'isAbstract'
            ]
        def go_deeper(child, sibling):
            if isinstance(child, dict):
                difference = {}
                if not sibling:
                    # omit empty (unset) values, but take care of zero, as it's not an empty value!
                    # also keep in mind that if key is not inherited, its default value is 'False'
                    # hence we can safely ignore 'key == False' in child
                    difference.update({
                        key: val for key, val in child.items()
                        if key in preserve or child.get(key) or child.get(key) == 0
                    })
                    return difference
                for key, val in child.items():
                    if key in sibling and val == sibling[key]:
                        if key in preserve:
                            difference.update({key: val})
                        elif key in ['account', 'gateway']:
                            payload = go_deeper(val, sibling[key])
                            if payload: # should not ever fall out of here, but anyway
                                difference.update({key: payload})
                        else:
                            continue
                    elif key in sibling and not isinstance(val, (dict, list)): # child[key] != sibling[key]
                        # compare if template corresponds to compiled value
                        if isinstance(val, str) and isinstance(sibling[key], dict) and sibling[key].get('base'):
                            sibling[key] = sibling[key]['base']
                        if isinstance(val, str) and isinstance(sibling[key], dict) and sibling[key].get('$template'):
                            sibling_val = self.sdbadds.lua_compile(instrument, sibling[key].get('$template'))
                            if val != sibling_val:
                                difference.update({key: val})
                        elif val is not None: # eliminate empty values
                            difference.update({key: val})
                    elif key in sibling: # val is dict or list
                        payload = go_deeper(val, sibling[key])
                        if payload:
                            difference.update({key: payload})
                    elif (val is not None and val is not False) or key in preserve: # key not in sibling and val is not empty
                        difference.update({key: val})
                return difference

            elif isinstance(child, list) and len(child) > 0:
                if not len(sibling): # nothing to inherit
                    return child
                if isinstance(child[0], list):
                    list_of_lists = []
                    for i in range(len(child)):
                        list_of_lists.append(go_deeper(child[i], sibling[i]))
                    return list_of_lists
                elif isinstance(child[0], dict): # assume that all entries in list are the same type
                    # firsly let's flatten the dicts
                    # we will use these artificial items to catch differencies in order or/and
                    # content and then call the real items to be compared and reduced
                    if not (child[0].get('account') or child[0].get('gateway')):
                        if len(child) != len(sibling):
                            return child
                        for i in range(len(child)):
                            if child[i] != sibling[i]:
                                return child
                        return None
                    reduced_list = []
                    flatten_child = []
                    flatten_sibling = []
                    for chi in child:
                        flatten_chi = {}
                        for chi_v in chi.values():
                            if isinstance(chi_v, str):
                                flatten_chi['route_id'] = chi_v
                            elif isinstance(chi_v, dict):
                                flatten_chi.update(chi_v)
                        flatten_child.append(flatten_chi)
                    for sib in sibling:
                        flatten_sib = {}
                        for sib_v in sib.values():
                            if isinstance(sib_v, str):
                                flatten_sib['route_id'] = sib_v
                            elif isinstance(sib_v, dict):
                                flatten_sib.update(sib_v)
                        flatten_sibling.append(flatten_sib)
                    # bad idea, needs futher elaboration
                    '''
                    # let's check if there are items in inherited, that are not in child
                    # if we delete them from list while editing assume that is:
                    # enabled = False
                    # allowFallback = False
                    for fsi in flatten_sibling:
                        if fsi['route_id'] not in [x['route_id'] for x in flatten_child]:
                            child.insert(0, deepcopy(sibling[flatten_sibling.index(fsi)]))
                            child[0][item_type].update({
                                'enabled': False,
                                'allowFallback': False
                            })
                            flatten_child.insert(0, fsi.copy())
                            flatten_child[0].update({
                                'enabled': False,
                                'allowFallback': False
                            })
                    '''
                    # now let's compare dicts next to each other with following considerations:
                    # · if both lists all the same, we write nothing (easy)
                    # · if there's some difference we write down all items from first
                    #   to the last that have changes (to preserve the order)
                    # · if child n-th route doesn't match with sibling n-th route
                    #   we seek this route in sibling and if found pop (x) it out of list:
                    #   c:  s: →    c:  s: →    c:  s:
                    #   C   A       C   A       C  (d)
                    #   A   B       A   B       A   A
                    #   B   C       B  (x)      B   B
                    #   D   D       D   D       D   D
                    #   
                    #   and insert the dummy (d) to the n-th place in sibling
                    # · we stop to write on the last route with difference

                    # firstly let's align lists and place the dummies
                    while True: # the cycle breaks when order of flatten_sibling is the same
                        moved = None
                        for i in range(len(flatten_child)):
                            if len(flatten_sibling) < i + 1 \
                                or flatten_child[i]['route_id'] != flatten_sibling[i]['route_id']:
                                moved = i
                                break
                        if moved is None:
                            break
                        flatten_child[moved].update({'moved': True})
                        # try to find a match if any and pop it out
                        match = next((num for num, x
                                in enumerate(flatten_sibling)
                                if x['route_id'] == flatten_child[moved]['route_id']), None)
                        if match: # move sibling to meet the child order
                            flatten_sibling.insert(moved, flatten_sibling.pop(match))
                        else: # place the dummy if child member is new
                            flatten_sibling.insert(moved, {'route_id': flatten_child[moved]['route_id']})
                    stop_write = None
                    # now let's catch the differencies
                    if len(flatten_child) > len(flatten_sibling):
                        stop_write = len(flatten_child)-1
                        # should not happen, but anyway
                    else:
                        # if the only difference is order, we will catch it
                        # thanks to new item in child {'moved': True}
                        # after the cycle is finished stop_write
                        # gets the index of the last child item that has to be written
                        for i in range(len(flatten_child)):
                            for key in flatten_child[i]:
                                if flatten_child[i][key] != flatten_sibling[i].get(key):
                                    # here we avoid to catch when child's key is False and no such key in sibling
                                    if flatten_child[i][key] == False and not flatten_sibling[i].get(key):
                                        continue
                                    else:
                                        stop_write = i
                                        break
                    if stop_write is not None:
                        for j in range(stop_write + 1):
                            sibling_to_compare = next((x for x in sibling
                                if flatten_child[j]['route_id'] in x.values()), None)
                            reduced_member = go_deeper(child[j], sibling_to_compare)
                            if reduced_member:
                                reduced_list.append(reduced_member)
                    if reduced_list:
                        return reduced_list
                    else:
                        return None
                elif set(child) != set(sibling):
                    return child
                else:
                    return None
            elif not sibling or child != sibling:
                return child
            else:
                return None
                    
        inherited = asyncio.run(self.sdbadds.build_inheritance(instrument))
        reduced_instrument = go_deeper(instrument, inherited)
        return reduced_instrument