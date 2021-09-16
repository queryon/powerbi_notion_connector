﻿// This file contains your Data Connector logic
section Notion;

[DataSource.Kind="Notion", Publish="Notion.Publish"]

shared Notion.Navigation = () =>
    let

        iterList = List.Generate(() =>  Value.Subtract(Notion.AmountOfDatabases(), 1), each _ > -1, each _ - 1),

        navHeader = {"Name", "Key", "Data", "ItemKind", "ItemName", "IsLeaf"}, 
        navInsights = List.Accumulate(iterList, {}, (state, current) => state & {{Notion.NameOfDatabase(current),  Notion.DatabaseID(current),  CreateNavTableV2(current), "Folder",    "Table",    false}}),

        objects = #table(navHeader, navInsights),

        NavTable = Table.ToNavigationTable(objects, {"Key"}, "Name", "Data", "ItemKind", "ItemName", "IsLeaf")
    in
        NavTable;

Notion = [
    Authentication = [
        Key = 
        [
            KeyLabel = "Notion API Key. See Queryon.com/notion"
        ]
    ],
    Label = Extension.LoadString("DataSourceLabel")
];



// Data Source UI publishing description
Notion.Publish = [
    Beta = true,
    Category = "Other",
    ButtonText = { Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://Queryon.com/notion",
    SourceImage = Notion.Icons,
    SourceTypeImage = Notion.Icons
];

Notion.Icons = [
    Icon16 = { Extension.Contents("Notion16.png"), Extension.Contents("Notion20.png"), Extension.Contents("Notion24.png"), Extension.Contents("Notion32.png") },
    Icon32 = { Extension.Contents("Notion32.png"), Extension.Contents("Notion40.png"), Extension.Contents("Notion48.png"), Extension.Contents("Notion64.png") }
];

Table.ToNavigationTable = (
    table as table,
    keyColumns as list,
    nameColumn as text,
    dataColumn as text,
    itemKindColumn as text,
    itemNameColumn as text,
    isLeafColumn as text
    ) as table =>
        let
            tableType = Value.Type(table),
            newTableType = Type.AddTableKey(tableType, keyColumns, true) meta 
            [
                NavigationTable.NameColumn = nameColumn,
                NavigationTable.DataColumn = dataColumn,
                NavigationTable.ItemKindColumn = itemKindColumn, 
                Preview.DelayColumn = itemNameColumn, 
                NavigationTable.IsLeafColumn = isLeafColumn
            ],
            navigationTable = Value.ReplaceType(table, newTableType)
        in
            navigationTable;

//-------------------------------------------------------------
[DataSource.Kind="Notion"]
shared Notion.HeaderContents = () =>
    let

        url = "https://api.notion.com/v1/databases",
        apiKey = Extension.CurrentCredential()[Key],
        headers = [

            #"Authorization"  = Text.Combine({"Bearer ", apiKey}),
            #"Content-Type"   = "application/json ; charset=utf-8",
            #"Notion-Version" = "2021-08-16"
        ],
        request = Web.Contents(url, [ Headers = headers, ManualCredentials = true ])
    in
        request;

shared Notion.DatabaseContents = (database_id) =>
    let
        first_part_of_url = Text.Combine({"https://api.notion.com/v1/databases/", database_id}),
        last_part_of_url  = Text.Combine({first_part_of_url, "/query"}),
        url = last_part_of_url,
        apiKey = Extension.CurrentCredential()[Key],
        headers = [
            
            #"Authorization"  = Text.Combine({"Bearer ", apiKey}),
            #"Content-Type"   = "application/json ; charset=utf-8",
            #"Notion-Version" = "2021-08-16"
        ],
        
  
        request = Web.Contents(url, [ Headers = headers, ManualCredentials = true , Content = Text.ToBinary("")])
    in
        request;


shared Notion.AmountOfDatabases = () =>
    let
        Source = Notion.Navigation(),
        Notion.NameOfDatabase = Notion.HeaderContents(),
        #"Imported JSON" = Json.Document(Notion.NameOfDatabase,65001),
        results = #"Imported JSON"[results]
    in
        List.Count(results);

shared Notion.NameOfDatabase = (num) =>
    let

        Source = Notion.Navigation(),
        Notion.NameOfDatabase = Notion.HeaderContents(),
        #"Imported JSON" = Json.Document(Notion.NameOfDatabase,65001),
        results = #"Imported JSON"[results],
        results1 = results{num},
        title = results1[title],
        title1 = title{0},
        plain_text = title1[plain_text]
    in
        plain_text;

shared Notion.DatabaseID = (num) =>
    let
        Source = Notion.Navigation(),
        Notion.NameOfDatabase = Notion.HeaderContents(),
        #"Imported JSON" = Json.Document(Notion.NameOfDatabase,65001),
        results = #"Imported JSON"[results],
        results1 = results{num},
        id = results1[id]
    in
        id;


shared Notion.DatabaseRecords = (num) =>
    let

        database_id = Notion.DatabaseID(num),

        Notion.DatabaseContents0 = Notion.DatabaseContents(database_id),
        #"Imported JSON" = Json.Document(Notion.DatabaseContents0,65001),
        #"Converted to Table" = Table.FromList(Notion.DatabaseContents0, Splitter.SplitByNothing(), null, null, ExtraValues.Error),



        list_results = #"Imported JSON"[results],
        table_toTable = Table.FromList(list_results, Splitter.SplitByNothing(), null, null, ExtraValues.Error),

        table_expandColumn1 = Table.ExpandRecordColumn(table_toTable, "Column1", {"properties"}, {"Column1.properties"}),

        table_PropertiesExpanded = Table.FromRecords(Table.Column(table_expandColumn1, "Column1.properties")),

        //ThisIsNull = #table({})

        list_PropertiesExpandedColumnName = Table.ColumnNames(table_PropertiesExpanded)
        
        //list_PropertiesExpandedColumnName1 = List.Generate(() => 1, each _ < 10, each _ + 1)
    in
        table_PropertiesExpanded;


shared Notion.DatabaseProperties = (num) =>
    let
        Notion.NameOfDatabase = Notion.DatabaseContents(Notion.DatabaseID(num)),
        #"Imported JSON" = Json.Document(Notion.NameOfDatabase,65001),
        results = #"Imported JSON"[results],
        results1 = results{num},
        properties = results1[properties]
    in
        properties;

shared ConvertToTable_SubTable = (database_records, name) =>
    let

        database_records_table = Table.FromList(database_records, Splitter.SplitByNothing(), null, null, ExtraValues.Error),

        database_records_table_expanded = Table.FromRecords(Table.Column(database_records_table, "Column1")),

        column_names = Table.ColumnNames(database_records_table_expanded),

        //database_records_table_expanded_deeper = Table.ExpandRecordColumn(database_records_table_expanded, column_names{2}, column_names{2})

        database_records_table_expanded_deeper = Table.ExpandListColumn(database_records_table_expanded, column_names{2}),

        result = if column_names{2} = "select" or column_names{2} = "number" then database_records_table_expanded else Table.ExpandListColumn(database_records_table_expanded, column_names{2}),

        result_removed_id   = Table.RemoveColumns(result, "id"),
        result_removed_type = Table.RemoveColumns(result_removed_id, "type"),

        //after_removed_expand = Table.FromRecords(Table.Column(result_removed_type, "title")),
        ending_column_names = Table.ColumnNames(result_removed_type),

        handle_types_check1 = if ending_column_names{0} = "multi_select" then HandleMultiSelect(result_removed_type, name) else Text.Combine({"Error 001, This fromat isnt currently supported :  ", ending_column_names{0}}),
        handle_types_check2 = if ending_column_names{0} = "rich_text"    then HandleRichText(result_removed_type, name)    else handle_types_check1,
        handle_types_check3 = if ending_column_names{0} = "title"        then HandleTitle(result_removed_type, name)       else handle_types_check2,
        handle_types_check4 = if ending_column_names{0} = "select"       then HandleSelect(result_removed_type, name)      else handle_types_check3,
        handle_types_check5 = if ending_column_names{0} = "number"       then HandleNumber(result_removed_type, name)      else handle_types_check4,

        iterList = List.Generate(() => Value.Subtract(Table.RowCount(handle_types_check5), 1), each _ > -1, each _ - 1),

        navHeader = {"Key", "Data"}, 
        navInsights = List.Accumulate(iterList, {}, (state, current) => 
            state & {{current, handle_types_check5{current}}} ),
        objects = #table(navHeader, navInsights),

        
        AllNewColumnNames = Table.ColumnNames(objects),
        expandedv2 = if ending_column_names{0} = "number" then Table.ExpandRecordColumn(objects, "Data", {"number"}, {name}) else Table.ExpandRecordColumn(objects, "Data", {name}, {name})
        //expandedv2 = Table.ExpandRecordColumn(objects, "Data", {"number"}, {name})
    in
        expandedv2;

shared HandleMultiSelect = (database_table, name) =>
    let
        Expanded = Table.ExpandRecordColumn(database_table, "multi_select", {"id", "name", "color"}, {"multi_select.id", name, "multi_select.color"}),
        Expanded_removed1 = Table.RemoveColumns(Expanded, "multi_select.id"),
        Expanded_removed2 = Table.RemoveColumns(Expanded_removed1, "multi_select.color")

        
    in
        Expanded_removed2;

shared HandleRichText = (database_table, name) =>
    let
        Expanded = Table.ExpandRecordColumn(database_table, "rich_text", {"plain_text"}, {name})
    in
        Expanded;

shared HandleTitle = (database_table, name) =>
    let
        Expanded = Table.ExpandRecordColumn(database_table, "title", {"plain_text"}, {name})
    in
        Expanded;

shared HandleSelect = (database_table, name) =>
    let
        Expanded = Table.ExpandRecordColumn(database_table, "select", {"name"}, {name})
    in
        Expanded;

shared HandleNumber = (database_table, name) =>
    let
        Expanded = Table.ExpandRecordColumn(database_table, "number", {"number"}, {name})
    in
        database_table;


CreateNavTableV2 = (num) as table => 
        let

        ColumnNames = Table.ColumnNames(Notion.DatabaseRecords(num)),

        AmountOfSubs = Value.Subtract(Table.ColumnCount(Table.FromRecords({Notion.DatabaseProperties(num)})), 1),

        iterList = List.Generate(() => AmountOfSubs, each _ > -1, each _ - 1),

        navHeader = {"Name", "Key", "Data", "ItemKind", "ItemName", "IsLeaf"}, 
        navInsights = List.Accumulate(iterList, {}, (state, current) => 
            state & {{ColumnNames{current}, current, ConvertToTable_SubTable(Table.Column(Notion.DatabaseRecords(num), ColumnNames{current}), ColumnNames{current}), "Table", "Table", true}} ),
        objects = #table(navHeader, navInsights),


        NavTable = Table.ToNavigationTable(objects, {"Key"}, "Name", "Data", "ItemKind", "ItemName", "IsLeaf")
    in
        NavTable;

shared Notion.NavigationV2 = () =>
    let

        iterList = List.Generate(() => Notion.AmountOfDatabases(), each _ > -1, each _ - 1),

        navHeader = {"Name", "Key", "Data", "ItemKind", "ItemName", "IsLeaf"}, 
        navInsights = List.Accumulate(iterList, {}, (state, current) => state & {{Notion.NameOfDatabase(current),  Notion.DatabaseID(current),  CreateNavTableV2(current), "Folder",    "Table",    false}}),
        objects = #table(navHeader, navInsights),

        NavTable = Table.ToNavigationTable(objects, {"Key"}, "Name", "Data", "ItemKind", "ItemName", "IsLeaf")
    in
        NavTable;