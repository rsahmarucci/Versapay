table 50010 "VersaPay Setup"
{
    DataClassification = ToBeClassified;

    fields
    {
        field(1; "Primary Key"; Code[20])
        {
        }
        field(2; "In Folder"; Text[250])
        {
        }
        field(3; "Out Folder"; Text[250])
        {
        }
        field(4; "Backup"; Boolean)
        {
        }
        field(5; "Auto Post"; Boolean)
        {
        }
        field(6; "Payments Journal Template"; Code[10])
        {
        }
        field(7; "Payments Journal Batch"; Code[10])
        {
        }
        field(8; "Fees Journal Template"; Code[10])
        {
        }
        field(9; "Fees Journal Batch"; Code[10])
        {
        }
        field(10; "File Delimeter"; Code[10])
        {
        }
        field(11; "No. Series Code"; Code[20])
        {
        }
        field(12; "Skip Non-Division Imports"; Boolean)
        {
        }
    }
    keys
    {
        key(Key1; "Primary Key")
        {
            Clustered = true;
        }
    }
}