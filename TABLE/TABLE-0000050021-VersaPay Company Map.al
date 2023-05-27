table 50021 "VersaPay Company Map"
{
    DataClassification = CustomerContent;
    DataPerCompany = false;

    fields
    {
        field(1; "Company Name"; Text[30])
        {
            TableRelation = "Company"."Name";
        }
        field(2; "VersaPay Invoice Division"; Code[20]) { }
    }
}