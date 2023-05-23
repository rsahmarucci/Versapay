page 50010 "VersaPay Setup"
{
    CaptionML = ENU = 'VersaPay Setup';
    SourceTable = "VersaPay Setup";
    PageType = Card;
    UsageCategory = Administration;
    ApplicationArea = Basic, Suite;
    // Special settings for Setup Cards
    DeleteAllowed = false;
    InsertAllowed = false;
    ModifyAllowed = true;
    RefreshOnActivate = true;

    layout
    {
        area(content)
        {
            group(General)
            {
                Caption = 'General';

                field("In Folder"; "In Folder")
                {
                }
                field("Out Folder"; "Out Folder")
                {
                }
                field("Backup"; "Backup")
                {
                }
                field("Auto Post"; "Auto Post")
                {
                }
                field("Skip Non-Division Imports"; "Skip Non-Division Imports")
                {
                }
                field("Payments Journal Template"; "Payments Journal Template")
                {
                }
                field("Payments Journal Batch"; "Payments Journal Batch")
                {
                }
                field("Fees Journal Template"; "Fees Journal Template")
                {
                }
                field("Fees Journal Batch"; "Fees Journal Batch")
                {
                }
                field("File Delimeter"; "File Delimeter")
                {
                }
                field("No. Series Code"; "No. Series Code")
                {
                }
            }
        }
    }
    actions
    {
        area(Creation)
        {
            group(Setup)
            {
                CaptionML = ENU = 'Setup';
                Visible = true;

                action(CreateSetup)
                {
                    CaptionML = ENU = 'Create Setup Record';
                    Image = CreateDocument;

                    trigger OnAction()
                    var
                        VersaSetup: Record "VersaPay Setup";
                    begin
                        IF NOT VersaSetup.GET then begin
                            VersaSetup.Init();
                            VersaSetup.Insert();
                        end;
                    end;
                }
            }
            group(Test)
            {
                CaptionML = ENU = 'Test';
                Visible = true;

                action(Customer)
                {
                    trigger OnAction()
                    var
                        VersaCod: Codeunit VersaPay;
                    begin
                        VersaCod.OutputFile('CUSTOMER');
                    end;
                }
                action(Invoice)
                {
                    trigger OnAction()
                    var
                        VersaCod: Codeunit VersaPay;
                    begin
                        VersaCod.OutputFile('INVOICE');
                    end;
                }
                /*                action(Payment)
                                {
                                    trigger OnAction()
                                    var
                                        VersaCod: Codeunit VersaPay;
                                    begin
                                        VersaCod.OutputFile('PAYMENT_OUT');
                                    end;
                                }
                                */
                action(Recon)
                {
                    trigger OnAction()
                    var
                        VersaCod: Codeunit VersaPay;
                    begin
                        VersaCod.OutputFile('RECON');
                    end;
                }
            }
        }
        area(Processing)
        {
            group(Process)
            {
                CaptionML = ENU = 'Process';
                Visible = true;

                action(UploadInFile)
                {
                    ApplicationArea = All;
                    CaptionML = ENU = 'Upload Single File';
                    Image = ImportLog;
                    Promoted = true;
                    PromotedCategory = Process;
                    PromotedIsBig = true;
                    ToolTip = 'This allows you to import a single file directly for Testing/Repair.';
                    Visible = true;

                    trigger OnAction()
                    var
                        VersaCod: Codeunit "VersaPay";
                        FileMgt: Codeunit "File Management"; // Won't exist for SAAS - will need to be moved to Blob bable
                        TempFile: Text;
                        TempFileType: Text;
                    begin
                        // This should use the 'Upload' function to test posting in a single file (and to allow web use)
                        TempFile := FileMgt.ServerTempFileName('csv');
                        Upload('Upload Import Versapay File', '', '', '', TempFile);
                        TempFileType := VersaCod.DetectType(TempFile);
                        VersaCod.InputFile(TempFile, TempFileType);
                    end;
                }
                action(ProcessInFiles)
                {
                    ApplicationArea = All;
                    CaptionML = ENU = 'Process In Files';
                    Image = Import;
                    Promoted = true;
                    PromotedCategory = Process;
                    PromotedIsBig = true;
                    ToolTip = 'This will process incoming files.';
                    Visible = true;

                    trigger OnAction()
                    var
                        VersaCod: Codeunit "VersaPay";
                    begin
                        // This should scan and process any files found:
                        VersaCod.FindFiles;
                    end;
                }
                action(CreateOutFiles)
                {
                    ApplicationArea = All;
                    CaptionML = ENU = 'Create Out Files';
                    Image = Export;
                    Promoted = true;
                    PromotedCategory = Process;
                    PromotedIsBig = true;
                    ToolTip = 'This will Create a set of outgoing files.';
                    Visible = true;

                    trigger OnAction()
                    var
                        VersaCod: Codeunit "VersaPay";
                    begin
                        VersaCod.OutFiles;
                    end;
                }
            }
        }
    }
}