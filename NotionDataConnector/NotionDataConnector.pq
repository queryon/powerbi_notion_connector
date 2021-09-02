// This file contains your Data Connector logic
section NotionDataConnector;

[DataSource.Kind="NotionDataConnector", Publish="NotionDataConnector.Publish"]
shared NotionDataConnector.Contents = (optional message as text) =>
    let

        url = "https://api.notion.com/v1/pages/c888a0f04b82419e85b77cc47862fe26",
        apiKey = Extension.CurrentCredential()[Key],
        headers = [

            #"Authorization"  = Text.Combine({"Bearer ", apiKey}),
            #"Content-Type"   = "application/json ; charset=utf-8",
            #"Notion-Version" = "2021-08-16"
        ],
        request = Web.Contents(url, [ Headers = headers, ManualCredentials = true ])
    in
        request;

NotionDataConnector = [
    TestConnection = (dataSourcePath) => {"NotionDataConnector.Contents"},
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
