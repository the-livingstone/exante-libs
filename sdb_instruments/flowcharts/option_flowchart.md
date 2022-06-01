## Option init
```mermaid
flowchart TB
    Option(["Option.__init__"])
    --> opt_init_req[/"
        ticker
        exchange
        week_number=0
        recreate=False
        env='prod'
    "/]
    --> opt_init_opt[/"
        Optional:
        shortname
        option_type
        underlying
        parent_folder
    "/]
    --> opt_init[/"
        Control switches:
        recreate=False
        silent=False
    "/]
    --> super_init[["super().__init__"]]

    super_init_args["
        instrument={}
        instrument_type='OPTION'

        ticker
        exchange
        shortname
        parent_folder
        week_number
        option_type
        parent_tree
        env

        recreate
        silent
    "] -.-> |args| super_init
    --> opt_type{"what option_type?"}

    super_init -.-> |sets| super_set["
        self.instrument
        self.parent_folder
        self.weekly_commons
        self.reference
        self.contracts
    "]

    opt_type --> |OPTION| set_udl[["self.set_underlying"]]
    set_udl_args[self.underlying] -.->|args| set_udl
    set_udl -.-> |sets| set_udl_set["self.underlying_dict"]
    opt_type --> |OPTION ON FUTURE| align_la_lt[["self._align_expiry_la_lt"]]
    opt_type --> |Other| opt_runtime_error([RuntimeError])
    align_la_lt_args["
        self.contracts
        self.update_expirations
    "] -.-> |args| align_la_lt
    set_udl --> align_la_lt
    -.-> |sets| set_exp_la_lt["
        self.contracts[x].instrument['lastAvailable'] 
        self.contracts[x].instrument['lastTrading']
    "]
    align_la_lt --> finish([End])


```

## Option: super.\_\_init\_\_
```mermaid
flowchart TB
    instr(["Instrument.__init__"])
    --> super_params_const[/"
        const:
        instrument={}
        instrument_type='OPTION'
    "/]
    --> super_params_opt[/"
        option specific:
        ticker
        exchange
        shortname
        parent_folder
        week_number
        option_type
        parent_tree
        env
    "/]
    --> super_params_switch[/"
        control_switches:
        recreate=False
        silent=False

    "/]
    --> load_tree("sdbadds.load_tree()")
    --> is_parent_folder{self.parent_folder?}
    --> |str| parent_str("
        self.parent_folder
        = self.sdb.get(self.parent_folder)
    ")
    is_parent_folder --> |dict| parent_dict("
        self.parent_folder_id
        = self.parent_folder.get('_id')
    ")
    is_parent_folder --> |None| find_parent_folder[["
        self.parent_folder_id
        = _find_parent_folder()
    "]]
    --> set_pf["set self.parent_folder"]
    parent_str --> is_parent_sdb_get{"
        is self.parent_folder?
        is abstract?
    "}
    --> |yes| set_pf_id("
        self.parent_folder_id
        = self.parent_folder['_id']
    ")
    --> set_pf
    parent_dict --> set_pf
    is_parent_sdb_get --> |No| RuntimeError([RuntimeError])
    set_pf --> find_series[["
        self.instrument,
        self.reference,
        self.contracts,
        self.weekly_commons
        = self._find_series()
    "]]
    --> finish([End])


```
## _find_parent_folder
Parent folder is not the direct parent for option series, it's just root -> OPTION (or OPTION ON FUTURE) -> exchange folder  
direct parent could be situated deeper (e.g. Root -> OPTION ON FUTURE -> CME -> Agriculture -> HE)
```mermaid
flowchart TB
    find_parent_folder(["_find_parent_folder()"])
    --> try_opt_type["
        x:
        self.option_type
        OPTION
        OPTION ON FUTURE
    "]
    --> is_next_opt_type{is next x?}

    is_next_opt_type --> |no| poss_exch
    is_next_opt_type --> |yes| get_pf_uuid_none("
        self.parent_folder_id
        = sdb.get_uuid_by_path(
        'Root',
        x,
        self.exchange)")
    --> is_pf{is self.parent_folder_id?}
    --> |yes| set_opt_type(self.option_type = x)
    --> finish_set
    poss_exch("
        possible_exchanges 
        = [x['exchangeId'] for x
        in sdb.get_exchanges()
        if x['exchangeName'] == self.exchange]
    ")
    --> get_opt_id("
        opt_id 
        = sdb.get_uuid_by_path(['Root', 'OPTION'])
    ")
    --> get_oof_id("
        oof_id
        = sdb.get_uuid_by_path(['Root', 'OPTION ON FUTURE'])
    ")
    get_oof_id --> get_opt_exch("
        exch_folders
        = sdb.get_heirs(opt_id) +
        sdb.get_heirs(oof_id)
    ")
    get_opt_exch --> get_poss_exch("
        possible_exchange_folders
        = [x for x in exchange_folders 
        if x['exchangeId'] in possible_exchanges]
    ")
    get_poss_exch --> how_many_poss_exch{"len(possible_exchange_folders)?"}
    how_many_poss_exch --> |0| err_exch_not_exist(["
        RuntimeError:
        'exchange does not exist'
    "])
    how_many_poss_exch --> |1| set_pf_from_poss("
        self.parent_folder_id
        = possible_exchange_folders[0]['_id']
    ")
    how_many_poss_exch --> |>1| get_ticker_folders("
        ticker_folders
        = [x for pef in possible_exchange_folders for x
        in sdb.get_heirs(pef['_id'], recursive=True)
        if x['name'] == self.ticker]
    ")
    --> len_tf{"len(ticker_folders)?"}
    len_tf --> |>1 or <1| err_cannot_select(["
        RuntimeError:
        'Cannot decide'
    "])
    len_tf --> |1| tf_set_o_type("
        self.option_type
        = next(x['name'] for x in self.tree
        if x['_id'] == ticker_folders[0]['path'][1])
    ")
    tf_set_o_type --> tf_set_pf("
        self.parent_folder_id
        = ticker_folders[0]['path'][2]
    ")

    set_pf_from_poss --> finish_set
    tf_set_pf --> finish_set
    is_pf --> |no| try_opt_type
    finish_set[set self.parent_folder_id]
    --> finish([End])
    
```
## \_find_series
```mermaid
flowchart TB
    find_series["_find_series()"]
    find_series --> instr_id("
        instr_id
        = next((x['_id'] for x in self.tree
        if self.parent_folder_id in x['path']
        and x['name'] == self.ticker), None)
    ")
    instr_id --> set_instrument("
        instrument
        = sdb.get(instr_id)) if instr_id else {}
    ")
    set_instrument --> set_reference("
        reference
        = deepcopy(self.instrument)
    ")
    set_reference --> is_instrument{"is instrument?"}
    is_instrument --> |yes| is_parent_tree{"is parent_tree?"}
    --> |yes| par_ser_tree("
        self.series_tree
        = [x for x
        in self.parent_tree
        if instrument['_id'] in x['path']]
    ")
    --> set_contract_ids
    is_parent_tree --> |No| heirs_ser_tree("
        self.series_tree
        = sdb.get_heirs(
        instrument['_id'],
        full=True,
        recursive=True)
    ")
    --> append_instr("self.series_tree.append(instrument)")
    --> set_contract_ids("
        contract_ids
        = [x for x in self.tree
        if x['path'][:-1] == self.instrument['path']
        and not x['isAbstract']]
    ")
    --> set_contracts[["
        contracts
        = [OptionExpiration(self, payload=x) for x
        in self.series.tree]
    "]]
    --> is_weeknum{"is self.week_number?"}
    is_weeknum --> |0| select_weekly_folders("
        weekly_folders
        = [x for x in self.series_tree
        if x['path'][:-1] == instrument['path']
        and 'weekly' in x['name'].lower()
        and x['isAbstract']]
    ")
    --> set_weekly_commons[["
        weekly_commons
        = [WeeklyCommon(self, uuid=x['_id']) for x
        in weekly_folders]
    "]]
    --> set_all

    is_weeknum --> |>0| set_weekly_commons_empty("weekly_commons = []")
    --> set_all
    is_instrument --> |no| is_shortname{"is self.shortname?"}
    is_shortname --> |No| err_no_instr(["
        NoInstrumentError:
        series does not exist in SymbolDB
    "])
    is_shortname --> |Yes| set_wcommons_empty("weekly_commons = []")
    set_wcommons_empty --> set_contracts_empty("contracts = []")
    set_contracts_empty --> set_instrument_create[["
        instrument
        = self.create_series_dict
    "]]
    --> set_all[/"
        return:
        instrument
        reference
        contracts
        weekly_commons
    "/]
    --> finish(["End"])

```
## OptionExpiration.\_\_init\_\_
```mermaid
flowchart TB
    OptionExpiration(["OptionExpiration.__init__"])
    --> opt_init_req[/"
        option: Option
        expiration_date [str, date, datetime] = None
        strikes: dict = None
        maturity: str = None
        payload: dict = None
        expiration_underlying: str = None 
    "/]

    --> super_init[["super().__init__"]]
    super_init_args["
        instrument={}
        instrument_type='OPTION'
        env
    "] -.-> |args| super_init
    --> self_instr("
        self.instrument
        = payload
    ")
    --> is_exp_date{"is expiration_date?"}
    --> |yes| exp_date_format{"
        expiration date
        format?
    "}
    is_exp_date --> |no| is_instr_exp
    exp_date_format --> |str| date_str("
        try:
        self.expiration
        = dt.date.fromisoformat(
            expiration_date)
    ")
    --> |ValueError| is_instr_exp{"is self.instrument?"}
    --> |no| exp_err(["
        ExpirationError:
        Invalid expiration date format
    "])
    is_instr_exp --> |yes| exp_from_instr("
        self.expiration
        = sdb.sdb_to_date(
        self.instrument['expiry'])
    ")
    --> set_exp
    exp_date_format --> |date| date_date("
        self.expiration
        = expiration_date
    ")
    --> set_exp
    exp_date_format --> |datetime| date_dt("
        self.expiration
        = expiration_date.date()
    ")
    --> set_exp["set self.expiuration"]
    set_exp --> if_mat{"is maturity?"}
    --> |yes| format_mat_str[["
        self.maturity
        = self.format_maturity(
            maturity)
    "]]
    --> set_maturity["set self.maturity"]
    if_mat --> |no| is_instr_mat{"
        is maturityDate
        in self.instrument?
    "}
    --> |yes| format_instr_mat[["
        self.maturity
        = self.format_maturity(
            self.instrument['maturityDate'])
    "]]
    --> set_maturity
    is_instr_mat --> |no| set_mat_from_exp("
        self.maturity
        = self.expiration.strftime('%Y-%m')
    ")
    --> set_maturity{"is self.maturity?"}

    comp_prnt_args["
        [self.option.compiled_parent,
        self.option.instrument]
        include_self=True
    "] -.-> |args| comp_prnt[["
        self.compiled_parent
        = sdbadds.build_inheritance
            
    "]]
    set_maturity --> |no| exp_err_mat(["
        ExpirationError:
        Cannot format maturity
    "])
    set_maturity --> |yes| comp_prnt
    comp_prnt --> is_instr{"is self.instrument?"}
    is_instr --> |yes| is_path
    is_instr --> |no| is_strikes{"is strikes?"}
    is_strikes --> |yes| mk_dict[["
        build_expiration_dict(strikes)
    "]]
    is_strikes --> |no| exp_err_str(["
        ExpirationError:
        Strikes dict is required
    "])
    mk_dict --> is_path{"
        is path
        in self.instrument?
    "}
    mk_dict -.-> |sets| set_self_instr["self.instrument"]
    is_path --> |no| set_path("
        self.instrument['path']
        = deepcopy(option.instrument['path'])
    ")
    --> is_id_in_opt{"
        is _id in
        option.instrument?
    "}
    --> |no| set_fake_id("
        self.instrument['path'].append('new series id')
    ")
    --> set_def_str("self.instrument.setdefault('strikePrices', {})")
    --> set_def_put("self.instrument['strikePrices'].setdefault('PUT', [])")
    --> set_def_cal("self.instrument['strikePrices'].setdefault('CALL', [])")
    is_id_in_opt --> |yes| set_def_str
    is_path --> |yes| set_def_str
    set_def_cal --> is_strikess{"
        is strikes?
    "}
    --> |no| is_oof{"
        is OPTION ON FUTURE?
    "}
    is_strikess --> |yes| set_strikes[["
        self.add_strikes(strikes)
    "]] --> is_oof
    is_oof --> |no| finish(["End"])
    is_oof --> |yes| set_oof_udl[["
        self.set_underlying_future(
        expiration_underlying)
    "]]
    --> finish
```
## WeeklyCommon.\_\_init\_\_
```mermaid
flowchart TB
    WeeklyCommon(["WeeklyCommon.__init__"])
    --> opt_init_req[/"
        option: Option
        payload: dict = None
        uuid: str = None
        common_name: str = 'Weekly'
        templates: dict = None 
    "/]
    --> is_uuid{"is uuid?"}
    --> |yes| get_payload("
        self.payload
        = sdb.get(uuid)")
    is_uuid --> |no| is_payload
    get_payload --> is_payload{"is payload?"}
    --> |no| set_payload("
        self.payload
        = {
            'isAbstract': True,
            'path': self.option.instrument['path'],
            'name': common_name
        }
    ")
    --> set_ref("
        self.reference
        = deepcopy(self.payload)
    ")
    is_payload --> |yes| set_ref

```