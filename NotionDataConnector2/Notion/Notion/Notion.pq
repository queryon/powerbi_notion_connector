// This file contains your Data Connector logic
section Notion;

[DataSource.Kind="Notion", Publish="Notion.Publish"]

shared Notion.Navigation = () =>

    let

        iterList = List.Generate(() =>  Value.Subtract(Notion.AmountOfDatabases(), 1), each _ > -1, each _ - 1),

        navHeader = {"Name", "Key", "Data", "ItemKind", "ItemName", "IsLeaf"}, 
        navInsights = List.Accumulate(iterList, {}, (state, current) => state & {{GetNameOfTable(current),  Notion.DatabaseID(current),  CreateNavTable(current), "Folder",    "Table",    true}}),

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
        title;

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

        list_PropertiesExpandedColumnName = Table.ColumnNames(table_PropertiesExpanded)
        
    in
        table_PropertiesExpanded;


shared Notion.DatabaseProperties = (num) =>
    let
        Notion.NameOfDatabase = Notion.DatabaseContents(Notion.DatabaseID(num)),
        #"Imported JSON" = Json.Document(Notion.NameOfDatabase,65001),
        results = #"Imported JSON"[results],
        results1 = results{0},
        properties = results1[properties]
    in
        properties;


shared AddNotionKey = (database, name, SPECIFYNAME) =>
    let

        iterList = List.Generate(() => Value.Subtract(Table.RowCount(database), 1), each _ > -1, each _ - 1),

        navHeader = {"Notion_Key", "Data"}, 
        navInsights = List.Accumulate(iterList, {}, (state, current) => 
            state & {{current, database{current}}} ),
        objects = #table(navHeader, navInsights),

        Expanded = Table.ExpandRecordColumn(objects, "Data", {SPECIFYNAME}, {name})

    in
        Expanded;

shared ConvertToTable_SubTableV2 = (database_records, name) =>
    let

        database_records_table          = Table.FromList(database_records, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
        database_records_table_expanded = Table.FromRecords(Table.Column(database_records_table, "Column1")),

        Database_RemovedColumns = RemoveUneccessaryColumns(database_records_table_expanded),

        Database_ColumnNames = Table.ColumnNames(Database_RemovedColumns),

        Check_1 = if Database_ColumnNames{0} = "title"        then HandleTitleFormat(Database_RemovedColumns, name, "Data")              else null,
        Check_2 = if Database_ColumnNames{0} = "number"       then HandleNumberFormat(Database_RemovedColumns, name, "number")           else Check_1,
        Check_3 = if Database_ColumnNames{0} = "rich_text"    then HandleRichTextFormat(Database_RemovedColumns, name, "rich_text")      else Check_2,
        Check_4 = if Database_ColumnNames{0} = "select"       then HandleSelectFormat(Database_RemovedColumns, name, "select")           else Check_3,
        Check_5 = if Database_ColumnNames{0} = "checkbox"     then HandleCheckBoxFormat(Database_RemovedColumns, name, "checkbox")       else Check_4,
        Check_6 = if Database_ColumnNames{0} = "multi_select" then HandleMultiSelectFormat(Database_RemovedColumns, name, "multi_select")   else Check_5

    in
        Check_6;


shared HandleMultiSelect = (database_table, name) =>
    let
        Expanded = Table.ExpandRecordColumn(database_table, "multi_select", {"id", "name", "color"}, {"multi_select.id", name, "multi_select.color"}),
        Expanded_removed1 = Table.RemoveColumns(Expanded, "multi_select.id"),
        Expanded_removed2 = Table.RemoveColumns(Expanded_removed1, "multi_select.color")

        
    in
        Expanded;


GetRowData = (table, RowIter, ColumnName) =>
    let
        column = Table.Column(table, ColumnName),
        end2 = column{RowIter}
        
    in
        end2;


GetDatatype = (database_records, num) =>
    let


        database_records_table          = Table.FromList(database_records, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
        database_records_table_expanded = Table.FromRecords(Table.Column(database_records_table, "Column1")),
        Database_RemovedColumns = RemoveUneccessaryColumns(database_records_table_expanded),
        Database_ColumnNames = Table.ColumnNames(Database_RemovedColumns),


        Check_1 = if Database_ColumnNames{0} = "title"        then type text    else type text,
        Check_2 = if Database_ColumnNames{0} = "number"       then Int64.Type   else Check_1,
        Check_3 = if Database_ColumnNames{0} = "rich_text"    then type text    else Check_2,
        Check_4 = if Database_ColumnNames{0} = "select"       then type text    else Check_3,
        Check_5 = if Database_ColumnNames{0} = "checkbox"     then type logical else Check_4,
        Check_6 = if Database_ColumnNames{0} = "multi_select" then type text    else Check_5


    in
        Check_6;

CreateNavTable = (num) as table => 
    let

        ColumnNames     = Table.ColumnNames(Notion.DatabaseRecords(num)),

        //Get Data Types
        DataTypeListiterListAmountOfSubs2 = Value.Subtract(Table.ColumnCount(Table.FromRecords({Notion.DatabaseProperties(num)})), 1),
        DataTypeListiterList = List.Generate(() => DataTypeListiterListAmountOfSubs2, each _ > -1, each _ - 1),
        DataTypeList = List.Accumulate(DataTypeListiterList, {}, (state, current) => state &  {GetDatatype(Table.Column(Notion.DatabaseRecords(num), ColumnNames{current}), ColumnNames{current})}),
   
       
        AmountOfSubs2 = Value.Subtract(Table.ColumnCount(Table.FromRecords({Notion.DatabaseProperties(num)})), 1),

        iterList = List.Generate(() => AmountOfSubs2, each _ > -1, each _ - 1),

        navHeader = {"Data"}, 
        navInsights = List.Accumulate(iterList, {}, (state, current) => 
            state & {{ConvertToTable_SubTableV2(Table.Column(Notion.DatabaseRecords(num), ColumnNames{current}), ColumnNames{current})}} ),
        objects = #table(navHeader, navInsights),



        //------------------------



        //Get Row Amount 
        rowAmountBefore = Value.Subtract(Table.RowCount(objects{0}[Data]), 1),

        //Get Column Amount 
        columnAmountBefore = Table.ColumnCount(Notion.DatabaseRecords(num)),


        //The start of the Join columns
        reveredColumnNames = List.Reverse(ColumnNames),


        rowAmount = List.Generate(() => rowAmountBefore, each _ > -1, each _ - 1),

        columnAmount = List.Generate(() => Value.Subtract(columnAmountBefore, 1), each _ > -1, each _ - 1),


        CombinedJoinedColumnsTable_List = List.Accumulate(rowAmount, {}, (state, i_column) => 
            state & {
                List.Accumulate(columnAmount, {}, (state, i_row) =>  state & 
                { 
                    GetRowData(objects{i_row}[Data], i_column, reveredColumnNames{i_row})
                })
            }),


        CombinedobjectsToTable = #table(ColumnNames, CombinedJoinedColumnsTable_List),

        CreateTransformColumnTypesList_Iter = List.Generate(() => AmountOfSubs2, each _ > -1, each _ - 1),
        CreateTransformColumnTypesList      = List.Accumulate(CreateTransformColumnTypesList_Iter, {}, (state, current) =>  state & {{     reveredColumnNames{current}, DataTypeList{current}     }} ),

        Correction0 = Table.TransformColumnTypes(CombinedobjectsToTable, CreateTransformColumnTypesList)

    in
        Correction0;


shared GetDataFromListOfTables = (listindex, name) =>
    let
        tabled = Table.FromList(listindex, Splitter.SplitByNothing(), null, null, ExtraValues.Error),

        Columnnames = Table.ColumnNames(tabled),
        Expanded = Table.ExpandTableColumn(tabled, "Column1", {"Notion_Key", name}, {"Notion_Key", "Data"})
    in
        Expanded;





























//Handle title Text ----------------------------------------------------------------------------------------------------

shared GetPlainText = (database, num) =>
    let
        title1 = database{num},
        plain_text = title1[plain_text]

        
    in
        plain_text;

shared RemoveUneccessaryColumns = (database) =>
    let
        Data = database,
        RemovedColumns = Table.RemoveColumns(Data,{"id", "type"})
    in
        RemovedColumns;

shared ForEveryTableCheckForDup = (database, name) =>
    let

        AddedTable = Table.AddColumn(database, "Duplicate Amount", each List.Count([title]))
    in
        AddedTable;

shared HandleTitleFormat = (database, name, SPECIFYNAME) =>
    let

        CombinedTable  = Table.AddColumn(database, "First",       each GetPlainText([title], 0)), //temp create columns to add things together 
        CombinedTable1 = Table.AddColumn(CombinedTable, "Second", each GetPlainText([title], 1)),

        ReplaceErrorsWithSpaces = Table.ReplaceErrorValues( CombinedTable1,  {"Second", " "}),

        CombinedTable2 = Table.AddColumn(ReplaceErrorsWithSpaces, "Data", each Text.Combine({[First], [Second]})),

        RemoveFirst  = Table.RemoveColumns(CombinedTable2, {"First", "Second"} ), //remove temp columns

        iterList = List.Generate(() => Value.Subtract(Table.RowCount(database), 1), each _ > -1, each _ - 1),
        
        //Add Key
        navHeader = {"Notion_Key", "Data"}, 
        navInsights = List.Accumulate(iterList, {}, (state, current) => 
            state & {{current, RemoveFirst{current}}} ),
        objects = #table(navHeader, navInsights),

        Expanded = Table.ExpandRecordColumn(objects, "Data", {SPECIFYNAME}, {name})
        
    in
        Expanded;


//Handle Number Format ----------------------------------------------------------------------------------------------------


shared HandleNumberFormat = (database, name, SPECIFYNAME ) =>
    let

        iterList = List.Generate(() => Value.Subtract(Table.RowCount(database), 1), each _ > -1, each _ - 1),

        navHeader = {"Notion_Key", "Data"}, 
        navInsights = List.Accumulate(iterList, {}, (state, current) => 
            state & {{current, database{current}}} ),
        objects = #table(navHeader, navInsights),

        Expanded = Table.ExpandRecordColumn(objects, "Data", {SPECIFYNAME}, {name})


    in
        Expanded;

//Handle Rich Text Format ----------------------------------------------------------------------------------------------------

shared HandleRichTextFormat = (database, name, SPECIFYNAME ) =>
    let

        iterList = List.Generate(() => Value.Subtract(Table.RowCount(database), 1), each _ > -1, each _ - 1),

        navHeader = {"Notion_Key", "Data"}, 
        navInsights = List.Accumulate(iterList, {}, (state, current) => 
            state & {{current, database{current}}} ),
        objects = #table(navHeader, navInsights),

        Expanded = Table.ExpandRecordColumn(objects, "Data", {SPECIFYNAME}, {name}),

        ExpandedWorkScore = Table.ExpandListColumn(Expanded, name),
        ExpandedWorkScore1 = Table.ExpandRecordColumn(ExpandedWorkScore, name, {"plain_text"}, {name})

    in
        ExpandedWorkScore1;

//Handle Select Format ----------------------------------------------------------------------------------------------------


shared HandleSelectFormat = (database, name, SPECIFYNAME ) =>
    let

        iterList = List.Generate(() => Value.Subtract(Table.RowCount(database), 1), each _ > -1, each _ - 1),

        navHeader = {"Notion_Key", "Data"}, 
        navInsights = List.Accumulate(iterList, {}, (state, current) => 
            state & {{current, database{current}}} ),
        objects = #table(navHeader, navInsights),

        Expanded = Table.ExpandRecordColumn(objects, "Data", {SPECIFYNAME}, {name}),
        Expanded2 = Table.ExpandRecordColumn(Expanded, name, {"name"}, {name})

    in
        Expanded2;



//Handle CheckBox Format ----------------------------------------------------------------------------------------------------


shared HandleCheckBoxFormat = (database, name, SPECIFYNAME ) =>
    let

        iterList = List.Generate(() => Value.Subtract(Table.RowCount(database), 1), each _ > -1, each _ - 1),

        navHeader = {"Notion_Key", "Data"}, 
        navInsights = List.Accumulate(iterList, {}, (state, current) => 
            state & {{current, database{current}}} ),
        objects = #table(navHeader, navInsights),

        Expanded = Table.ExpandRecordColumn(objects, "Data", {SPECIFYNAME}, {name})

    in
        Expanded;



//Handle Getting Title of Table-------------------------------------------------------------------------------------------
//This function was implemented because Titles of databases were getting cut off from spaces and special characters

shared GetNameOfTable = (num) =>
    let

        Source = Notion.Navigation(),
        Notion.NameOfDatabase = Notion.HeaderContents(),
        #"Imported JSON" = Json.Document(Notion.NameOfDatabase,65001),
        results = #"Imported JSON"[results],

        results1 = results{num},
        title = results1[title],

        Count = List.Count(title),

        text = if Count = 2 then Text.Combine({title{0}[plain_text], title{1}[plain_text]}) else title{0}[plain_text],
        
        title1 = title{0},

        CountOf = Table.RowCount(title),

        Check_2 = Text.Combine({title{0}[plain_text], title{1}[plain_text]}),

        plain_text = title1[plain_text]

    in
        text;

//Handle Multi Select ----------------------------------------------------------------------------------------------------

shared GetElementInList = (NumberOfMultiSelect, Field) =>
    let

        iterList = List.Generate(() =>  Value.Subtract(NumberOfMultiSelect, 1), each _ > -1, each _ - 1),
     
        ListOfSelected = List.Accumulate(iterList, {}, (state, current) => state & {{Field{current}}}),

        ToTable = Table.FromList(ListOfSelected, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
        ToTableExpand = Table.ExpandListColumn(ToTable, "Column1"),
        ToExpanndRecord = Table.ExpandRecordColumn(ToTableExpand, "Column1", {"name"}, {"Column1.name"})

        

    in
        Text.Combine(Table.ToList(ToExpanndRecord), ", ");
        
shared HandleMultiSelectFormat = (database, name, SPECIFYNAME) =>
    let

        CombinedTable2 = Table.AddColumn(database, "Combined_List", each GetElementInList(List.Count([multi_select]), [multi_select])),
        RemoveUneccessary = Table.RemoveColumns(CombinedTable2, "multi_select"),

        iterList = List.Generate(() => Value.Subtract(Table.RowCount(database), 1), each _ > -1, each _ - 1),

        navHeader = {"Notion_Key", "Data"}, 
        navInsights = List.Accumulate(iterList, {}, (state, current) => 
            state & {{current, RemoveUneccessary{current}}} ),
        objects = #table(navHeader, navInsights),

        Expanded = Table.ExpandRecordColumn(objects, "Data", {"Combined_List"}, {name})

    in
        Expanded;