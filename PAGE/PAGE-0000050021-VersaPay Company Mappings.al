page 50021 "VersaPay Company Mappings"
{
    Caption = 'VersaPay Company Mappings';
    SourceTable = "VersaPay Company Map";
    PageType = ListPlus;
    UsageCategory = Administration;
    ApplicationArea = Basic, Suite;
    DelayedInsert = true;
    RefreshOnActivate = true;

    layout
    {
        area(Content)
        {
            repeater(Companies)
            {
                Caption = 'Companies';
                field("Company Name"; Rec."Company Name") { }
                field("VersaPay Invoice Division"; Rec."VersaPay Invoice Division") { }
            }
        }
    }
}