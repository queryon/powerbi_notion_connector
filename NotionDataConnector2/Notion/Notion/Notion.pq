// This file contains your Data Connector logic
section Notion;

[DataSource.Kind="Notion", Publish="Notion.Publish"]

shared Notion.Navigation = () =>
    let
        objects = #table(
            {"Name",                    "Key",                   "Data",                                                     "ItemKind", "ItemName", "IsLeaf"},{
            {Notion.NameOfDatabase(0),  Notion.DatabaseID(0),    CreateNavTable(0), "Folder",    "Table",    false},
            {Notion.NameOfDatabase(1),  Notion.DatabaseID(1),    CreateNavTable(1), "Folder",    "Table",    false}
            }),
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
    LearnMoreUrl = "https://powerbi.microsoft.com/",
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
        Notion.NameOfDatabase = Source{[Key="Notion.NameOfDatabase"]}[Data],
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

shared ConvertToTable_SubTable = (database_records) =>
    let

        database_records_table = Table.FromList(database_records, Splitter.SplitByNothing(), null, null, ExtraValues.Error),

        database_records_table_expanded = Table.FromRecords(Table.Column(database_records_table, "Column1")),

        column_names = Table.ColumnNames(database_records_table_expanded),

        //database_records_table_expanded_deeper = Table.ExpandRecordColumn(database_records_table_expanded, column_names{2}, column_names{2})

        database_records_table_expanded_deeper = Table.ExpandListColumn(database_records_table_expanded, column_names{2}),

        result = if column_names{2} = "select" then database_records_table_expanded else Table.ExpandListColumn(database_records_table_expanded, column_names{2}),

        result_removed_id   = Table.RemoveColumns(result, "id"),
        result_removed_type = Table.RemoveColumns(result_removed_id, "type"),


        ending_column_names = Table.ColumnNames(result_removed_type),

        last_table_expand   = Table.FromRecords(Table.Column(result_removed_type, ending_column_names{0}))
    in
        last_table_expand;



CreateNavTable = (num) as table => 

    

    let

        ColumnNames = Table.ColumnNames(Notion.DatabaseRecords(num)),

        Tabled_Column_Data = ConvertToTable_SubTable(Table.Column(Notion.DatabaseRecords(num), ColumnNames{1})),

        objects = #table(
            {"Name",         "Key",   "Data",                                                    "ItemKind", "ItemName", "IsLeaf"},{

            {ColumnNames{0}, ColumnNames{0}, ConvertToTable_SubTable(Table.Column(Notion.DatabaseRecords(num), ColumnNames{1})), "Table",    "Table",    true},
            {ColumnNames{1}, ColumnNames{1}, ConvertToTable_SubTable(Table.Column(Notion.DatabaseRecords(num), ColumnNames{1})), "Table",    "Table",    true},
            {ColumnNames{2}, ColumnNames{2}, ConvertToTable_SubTable(Table.Column(Notion.DatabaseRecords(num), ColumnNames{2})), "Table",    "Table",    true},
            {ColumnNames{3}, ColumnNames{3}, ConvertToTable_SubTable(Table.Column(Notion.DatabaseRecords(num), ColumnNames{3})), "Table",    "Table",    true},
            {ColumnNames{4}, ColumnNames{4}, ConvertToTable_SubTable(Table.Column(Notion.DatabaseRecords(num), ColumnNames{4})), "Table",    "Table",    true}

        }),
        NavTable = Table.ToNavigationTable(objects, {"Key"}, "Name", "Data", "ItemKind", "ItemName", "IsLeaf")
    in
        NavTable;

CreateNavTablev2 = (num) as table => 

    let

        ColumnNames   = Table.ColumnNames(Notion.DatabaseRecords(num)),
        ListToIterate = List.Numbers(0, 2),

        objects = List.Accumulate(ListToIterate, 0, (state, current) => #table({"Name", "Key",   "Data", "ItemKind", "ItemName", "IsLeaf"},{ {ColumnNames{current}, ColumnNames{current}, Table.Column(Notion.DatabaseRecords(num), ColumnNames{current}), "Table",    "Table",    true} })),
        //objects2 = List.Accumulate(ListToIterate, 0, (state, current) => #table({"Name", "Key",   "Data", "ItemKind", "ItemName", "IsLeaf"},{ {ColumnNames{current}, ColumnNames{current}, Table.Column(Notion.DatabaseRecords(num), ColumnNames{current}), "Table",    "Table",    true} })),

        //combined = Table.Combine({objects, objects2}),

        CombinedNavTable = Table.ToNavigationTable(objects, {"Key"}, "Name", "Data", "ItemKind", "ItemName", "IsLeaf")
    in
        CombinedNavTable;


//list_PropertiesExpandedColumnName1 = List.Generate(() => 1, each _ < 10, each _ + 1) //Working ForLoop with generated list as work around??
