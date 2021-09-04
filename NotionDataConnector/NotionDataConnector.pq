// This file contains your Data Connector logic
section NotionDataConnector;

[DataSource.Kind="NotionDataConnector", Publish="NotionDataConnector.Publish"]

shared NotionDataConnector.Navigation = () =>
    let
        objects = #table(
            {"Name",                                  "Key",                               "Data",                           "ItemKind", "ItemName", "IsLeaf"},{
            {NotionDataConnector.NameOfDatabase(),  "NotionDataConnector.NameOfDatabase",   NotionDataConnector.Contents(), "Table",    "Table",    true},
            {NotionDataConnector.NameOfDatabase(),  "NotionDataConnector.asdasd",   NotionDataConnector.CreateNavigationDataRow(), "Table",    "Table",    true}
            }),
        NavTable = Table.ToNavigationTable(objects, {"Key"}, "Name", "Data", "ItemKind", "ItemName", "IsLeaf")
    in
        NavTable;


shared NotionDataConnector.FunctionCallThatReturnsATable = () =>
    #table({"DynamicColumn"}, {{"Dynamic Value"}});

NotionDataConnector = [
    TestConnection = (dataSourcePath) => {"NotionDataConnector.Navigation"},
    Authentication = [
        Key = 
        [
            KeyLabel = "Notion API Key"
        ]
    ],
    Label = Extension.LoadString("DataSourceLabel")
];


// Data Source UI publishing description
NotionDataConnector.Publish = [
    Beta = true,
    Category = "Other",
    ButtonText = { Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://powerbi.microsoft.com/",
    SourceImage = NotionDataConnector.Icons,
    SourceTypeImage = NotionDataConnector.Icons
];

NotionDataConnector.Icons = [
    Icon16 = { Extension.Contents("NotionDataConnector16.png"), Extension.Contents("NotionDataConnector20.png"), Extension.Contents("NotionDataConnector24.png"), Extension.Contents("NotionDataConnector32.png") },
    Icon32 = { Extension.Contents("NotionDataConnector32.png"), Extension.Contents("NotionDataConnector40.png"), Extension.Contents("NotionDataConnector48.png"), Extension.Contents("NotionDataConnector64.png") }
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

[DataSource.Kind="NotionDataConnector"]

shared NotionDataConnector.CreateNavigationDataRow = () =>
    let
        objects = #table(
            {"Name",  "Key",   "Data",                           "ItemKind", "ItemName", "IsLeaf"},{
            {"Item1", "item1", #table({"Column1"}, {{"123"}}), "Table",    "Table",    true},
            {"Item2", "item2", #table({"Column1"}, {{"2222"}}), "Table",    "Table",    true}
        }),
        NavTable = Table.ToNavigationTable(objects, {"Key"}, "Name", "Data", "ItemKind", "ItemName", "IsLeaf")
    in
        NavTable;

shared NotionDataConnector.NameOfDatabase = () =>
    let
        Source = NotionDataConnector.Navigation(),
        NotionDataConnector.NameOfDatabase = Source{[Key="NotionDataConnector.NameOfDatabase"]}[Data],
        #"Imported JSON" = Json.Document(NotionDataConnector.NameOfDatabase,65001),
        results = #"Imported JSON"[results],
        results1 = results{0},
        title = results1[title],
        title1 = title{0},
        plain_text = title1[plain_text]
    in
        plain_text;

[DataSource.Kind="NotionDataConnector"]
shared NotionDataConnector.DatabaseID = () =>
    let
        Source = NotionDataConnector.Navigation(),
        NotionDataConnector.NameOfDatabase = Source{[Key="NotionDataConnector.NameOfDatabase"]}[Data],
        #"Imported JSON" = Json.Document(NotionDataConnector.NameOfDatabase,65001),
        results = #"Imported JSON"[results],
        results1 = results{0},
        id = results1[id]
    in
        id;


//-------------------------------------------------------------
[DataSource.Kind="NotionDataConnector"]
shared NotionDataConnector.Contents = () =>
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





shared NotionDataConnector.DatabaseContents = () =>
    let

        url = "-X POST https://api.notion.com/v1/databases/1bf3bbe8594f402cb48f30e13bd8057b/query",
        apiKey = Extension.CurrentCredential()[Key],
        headers = [
            
            #"Authorization"  = Text.Combine({"Bearer ", apiKey}),
            #"Content-Type"   = "application/json ; charset=utf-8",
            #"Notion-Version" = "2021-08-16"
        ],
        request = Web.Contents(url, [ Headers = headers, ManualCredentials = true ])
    in
        request;


