// This file contains your Data Connector logic
section Notion;

[DataSource.Kind="Notion", Publish="Notion.Publish"]

shared Notion.Navigation = () =>
    let

        objects = #table(
            {"Name",                                  "Key",                               "Data",                           "ItemKind", "ItemName", "IsLeaf"},{
            {"Basic Raw Data",  "Notion.NameOfDatabase",           Notion.Contents(), "Table",    "Table",    true},
            {Notion.NameOfDatabase(0),  "Notion.NameOfDatabases",           Notion.DatabaseContents("1bf3bbe8594f402cb48f30e13bd8057b"), "Table",    "Table",    true},
            {Notion.NameOfDatabase(1),  "Notion.NameOfDatabases1",          Notion.DatabaseContents("bf7187cfd9b04f35b4859adc52174b08"), "Table",    "Table",    true}
            }),
        NavTable = Table.ToNavigationTable(objects, {"Key"}, "Name", "Data", "ItemKind", "ItemName", "IsLeaf")
    in
        NavTable;


shared Notion.FunctionCallThatReturnsATable = () =>
    #table({"DynamicColumn"}, {{"Dynamic Value"}});

Notion = [
    TestConnection = (dataSourcePath) => {"Notion.Navigation"},
    Authentication = [
        Key = 
        [
            KeyLabel = "Notion API Key"
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
shared Notion.Contents = () =>
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
        Notion.NameOfDatabase = Source{[Key="Notion.NameOfDatabase"]}[Data],
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
        Notion.NameOfDatabase = Source{[Key="Notion.NameOfDatabase"]}[Data],
        #"Imported JSON" = Json.Document(Notion.NameOfDatabase,65001),
        results = #"Imported JSON"[results],
        results1 = results{num},
        id = results1[id]
    in
        id;


