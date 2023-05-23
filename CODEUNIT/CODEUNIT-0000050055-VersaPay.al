codeunit 50055 "VersaPay"
{
    // Unfortunately, because Queries - while fast (as fast as nav data gets) - cannot manipulate data
    // We cannot use Queries to run this.  We will have to manually define the data pulls here.
    trigger OnRun()
    begin
        // This Funtion is just so you can run this from the Job queue to quickly enable.
        FindFiles;
        OutFiles;
    end;

    // THIS function must be replaced with a buffer if/when upgraded to SAAS (an on-premise REST service should insert files)
    // Then this function can be replaced with something that scans that file buffer in the cloud instead of on-premise folders
    procedure FindFiles()
    var
        VersaSetup: Record "VersaPay Setup";
        FileMgt: Codeunit "File Management";
        Files: Record "Name/Value Buffer" temporary;
        FileType: Text;
    Begin
        // This function is meant to scan the Input folder and try and detect a usable input file:
        // First, we'll need to collect some setup information:
        IF NOT VersaSetup.GET THEN ERROR('Could not get VersaPay Setup');

        FileMgt.GetServerDirectoryFilesList(Files, VersaSetup."In Folder");
        IF Files.FindFirst THEN
            REPEAT
                // Found a file - Lets Call Detect, then call Input:
                FileType := DetectType(Files.Name);
                InputFile(Files.Name, FileType);
            //MESSAGE('Found File ' + Files.Name);
            UNTIL Files.Next = 0;
    End;

    procedure OutFiles()
    begin
        // This function is to expose/allow outputting all files from the Job queue (by outputting all files)
        OutputFile('CUSTOMER');
        OutputFile('INVOICE');
        //OutputFile('PAYMENT_OUT'); //RPS-05182023 Commenting out as part of requirement
        OutputFile('RECON');
    end;

    // Input file Identifier Function (In case they aren't coded by filename) Returns Type
    procedure DetectType(FileName: Text) OutType: Text
    begin
        // We currently only have one type of input file, so for now just auto-return 'PAYMENT_IN'.
        // If they end up making more, we can add more delicate detection
        OutType := 'PAYMENT_IN';
    end;

    // Backup File - Must be replaced if moved to SAAS to work with the blob Table buffer.
    // This also will check the setup and DELETE the file if Backup isn't enabled.
    procedure BackupFile(Filename: Text)
    var
        VersaSetup: Record "VersaPay Setup";
        FileMgt: Codeunit "File Management";
        Path: Text;
        BackupPath: Text;
        BackupFile: Text;
    begin
        // This function should check for and create a backup dir if it doens't exist.
        // First, we'll need to collect some setup information:
        IF NOT VersaSetup.GET THEN ERROR('Could not get VersaPay Setup');

        IF VersaSetup.Backup THEN BEGIN
            // Then Backup is enabled:
            Path := FileMgt.GetDirectoryName(Filename);
            BackupPath := VersaSetup."In Folder" + '/Backup';
            BackupFile := BackupPath + '/' + FileMgt.GetFileName(FileName);
            //Message('Backup: ' + Filename + '->' + BackupFile);
            IF NOT FileMgt.ServerDirectoryExists(BackupPath) THEN begin
                FileMgt.ServerCreateDirectory(BackupPath);
            end;
            // Move the file
            FileMgt.MoveFile(Filename, BackupFile);
        END ELSE begin
            // Then we should just Delete the file.
            //Message('Delete: ' + filename);
            FileMgt.DeleteServerFile(Filename);
        end;
    end;

    // Input files are coded by the incoming file (to leave room for future expansion)
    procedure InputFile(FileName: Text; FileType: Text)
    var
        VersaSetup: Record "VersaPay Setup";
        Journal: Record "Gen. Journal Line";
        FeeJournal: Record "Gen. Journal Line";
        JournalCheck: Record "Gen. Journal Line";
        JournalHeader: Record "Gen. Journal Batch";
        NoSeries: Record "No. Series";
        NoSeriesMgt: Codeunit "NoSeriesManagement";
        Customer: Record "Customer";
        InFile: File;
        InStr: InStream;
        Buffer: Text;
        BufferVal: Variant;
        BufferDec: Decimal;
        Success: Boolean;
        LineNo: Integer;
        Header: Boolean;
        RowHeader: Dictionary of [Integer, Text]; // Using Dictionary to expose Count/Keys/Contains
        Row: Dictionary of [Integer, Text];
        RowReference: Text;
        Next_PayLineNo: Integer;
        Next_FeeLineNo: Integer;
    begin
        // First, we'll need to collect some setup information:
        IF NOT VersaSetup.GET THEN ERROR('Could not get VersaPay Setup');

        // Ready to check success
        Success := False;

        // Next, Lets open the file
        IF InFile.OPEN(FileName) THEN begin
            // Lets Process based on the type of File:
            CASE FileType of
                'PAYMENT_IN':
                    begin
                        // Some Quick tests to Error on before we begin (like missing setup)
                        // Test That the Fee and Payment Journal Batches exist, both needed for this import:
                        IF NOT JournalHeader.GET(VersaSetup."Payments Journal Template", VersaSetup."Payments Journal Batch") THEN ERROR('Payment Batch "' + VersaSetup."Payments Journal Template" + ':' + VersaSetup."Payments Journal Batch" + '" Could not be Found.  Please make sure it exists, and set it in VersaPay Setup');
                        IF NOT JournalHeader.GET(VersaSetup."Fees Journal Template", VersaSetup."Fees Journal Batch") THEN ERROR('Fees Batch "' + VersaSetup."Fees Journal Template" + ':' + VersaSetup."Fees Journal Batch" + '" Could not be Found.  Please make sure it exists, and set it in VersaPay Setup');
                        IF NOT NoSeries.GET(VersaSetup."No. Series Code") THEN ERROR('No. Series "' + VersaSetup."No. Series Code" + '" Could not be Found.  Please make sure it exists, and set it in VersaPay Setup');
                        // Release these as we don't technically need this.
                        JournalHeader.Reset;
                        NoSeries.Reset;
                        CLEAR(NoSeriesMgt); // Reset it so it'll count properly from here:

                        // For now, this only works with a Header.
                        // To allow it to process without a header, you can ignore the header and use NUMERIC mapping.
                        Header := True;
                        // Attach and Read:
                        InFile.CreateInStream(InStr);
                        LineNo := 1;

                        // If we are going to do a company based import - determine it here.

                        // Lock the Journal and get the next available number under this batch
                        Journal.LockTable;

                        // Get the next available number for each journal (Locked so it isn't interfered with):
                        Journal.Reset;
                        Journal.SetRange("Journal Template Name", VersaSetup."Payments Journal Template");
                        Journal.SetRange("Journal Batch Name", VersaSetup."Payments Journal Batch");
                        IF Journal.FindLast THEN
                            Next_PayLineNo := Journal."Line No." + 1000
                        ELSE
                            Next_PayLineNo := 1000;

                        Journal.Reset;
                        Journal.SetRange("Journal Template Name", VersaSetup."Fees Journal Template");
                        Journal.SetRange("Journal Batch Name", VersaSetup."Fees Journal Batch");
                        IF Journal.FindLast THEN
                            Next_FeeLineNo := Journal."Line No." + 1000
                        ELSE
                            Next_FeeLineNo := 1000;

                        WHILE NOT InStr.EOS DO begin
                            // This SHOULD pull a line at a time.
                            InStr.READTEXT(Buffer);

                            // If LineNo = 1, and we haven't disabled headers (in the setup) we can get Field names from here:
                            IF (LineNo = 1) AND Header THEN begin
                                // Header Line Process (Delimeters hardcoded for now):
                                // Headers will be used to match - since I don't trust them not to change the file randomly on us.
                                // (The Versa Team has shown they are incompetent at best, and malicious as worst)
                                CodedLineToArray(Buffer, ',', '"', RowHeader);
                                //MESSAGE('Found ' + FORMAT(RowHeader.Count) + ' Headers');

                                LineNo := LineNo + 1;
                            end
                            ELSE begin
                                // Reset Row:
                                Clear(Row);

                                // Normal Line Process
                                CodedLineToArray(Buffer, ',', '"', Row);

                                // First check:  Division.  If skip Division is true and this division <> Current Company - skip.
                                CodedLineValue(Row, CodedLineField(RowHeader, 'invoice_division'), BufferVal);
                                if (FORMAT(BufferVal) = CompanyName) OR (VersaSetup."Skip Non-Division Imports" = FALSE) THEN begin

                                    // Now, We will Create lines based on the data in the requested Journals:
                                    CodedLineValue(Row, CodedLineField(RowHeader, 'payment_reference'), BufferVal);
                                    RowReference := BufferVal;
                                    //Message('Processing reference: ' + RowReference + ' Columns ' + FORMAT(Row.Keys.Count) + ' Headers ' + FORMAT(RowHeader.Keys.Count));

                                    // First, Load this Customer so we can grab the Business Group Dimension AND be sure it exists in this company:
                                    CodedLineValue(Row, CodedLineField(RowHeader, 'customer_identifier'), BufferVal);
                                    IF NOT Customer.GET(FORMAT(BufferVal)) THEN ERROR('Could not Find Customer ' + FORMAT(BufferVal) + ' From Line Reference ' + RowReference);

                                    // Okay, we got this far, initialize the Output Line:
                                    Journal.Init;
                                    // Universal Hardcoded Values
                                    Journal."Journal Template Name" := VersaSetup."Payments Journal Template";
                                    Journal."Journal Batch Name" := VersaSetup."Payments Journal Batch";
                                    Journal."Account Type" := Journal."Account Type"::Customer;

                                    // Calculated Fields
                                    Journal."Line No." := Next_PayLineNo;
                                    Next_PayLineNo := Next_PayLineNo + 1000;

                                    // Now lets Render the Mapping:
                                    // Now Before we write the rest of the values, we need to slightly change up depending if this is part of a 'Credit' application:
                                    // If this a 'Credit', we have to handle it differently:
                                    // Credits are identified by the 'payment_method' of 'Credit':
                                    CodedLineValue(Row, CodedLineField(RowHeader, 'payment_method'), BufferVal);
                                    IF UPPERCASE(BufferVal) = 'CREDIT' THEN begin
                                        // For Credits, there are key differences:
                                        // Credits will import as multi-line instead of single-line depending on if they are 'Applied' or 'Used' in payment_note
                                        CodedLineValue(Row, CodedLineField(RowHeader, 'payment_note'), BufferVal);
                                        IF UPPERCASE(BufferVal) = 'CREDIT (APPLIED)' THEN begin
                                            // This creates the Applied part of a Document
                                            // The Applied Marks the INVOICE - so Applies-To Doc Type changes
                                            Journal."Applies-to Doc. Type" := Journal."Applies-to Doc. Type"::Invoice;
                                        end else begin
                                            // This creates the Used part of a Document.
                                            // The Used Marks the CREDIT - so Applies-To Doc Type Changes:
                                            Journal."Applies-to Doc. Type" := Journal."Applies-to Doc. Type"::"Credit Memo";
                                        end;
                                        // And there is no Balancing - so we need to match the Document No of the other side, if it has already been placed into the Journal:
                                        // (I.e. Look up if there exists an Applied to Document Number referencing this)
                                        JournalCheck.Reset;
                                        JournalCheck.SetRange("Journal Template Name", VersaSetup."Payments Journal Template");
                                        JournalCheck.SetRange("Journal Batch Name", VersaSetup."Payments Journal Batch");
                                        // They are linked by Reference - which is placed into the External Document Number:
                                        CodedLineValue(Row, CodedLineField(RowHeader, 'payment_reference'), BufferVal);
                                        JournalCheck.SetRange("External Document No.", 'VP - ' + FORMAT(BufferVal));
                                        IF JournalCheck.FINDLast THEN begin
                                            // Then we have an existing Document Number - use it here to balance the document:
                                            Journal."Document No." := JournalCheck."Document No.";
                                        END ELSE BEGIN
                                            // Then this may be the first one importing in a set - Just calculate one:
                                            //Journal."Document No." := NoSeriesMgt.TryGetNextNo(VersaSetup."No. Series Code", WorkDate());
                                            Journal."Document No." := NoSeriesMgt.DoGetNextNo(VersaSetup."No. Series Code", WorkDate(), False, False);
                                        end;
                                    END ELSE BEGIN
                                        // This Means they are NOT Credits - so Just do the normal Payment stuff:
                                        Journal."Bal. Account Type" := Journal."Bal. Account Type"::"Bank Account";
                                        Journal."Bal. Account No." := 'BA00055';
                                        Journal."Applies-to Doc. Type" := Journal."Applies-to Doc. Type"::Invoice;
                                        //Journal."Document No." := NoSeriesMgt.TryGetNextNo(VersaSetup."No. Series Code", WorkDate());
                                        Journal."Document No." := NoSeriesMgt.DoGetNextNo(VersaSetup."No. Series Code", WorkDate(), False, False);
                                    END;

                                    // Imported Fields - Are oddly the same for each type
                                    CodedLineValue(Row, CodedLineField(RowHeader, 'date'), BufferVal);
                                    Journal."Posting Date" := BufferVal; //date
                                    CodedLineValue(Row, CodedLineField(RowHeader, 'customer_identifier'), BufferVal);
                                    Journal."Account No." := BufferVal; //customer_identifier
                                    CodedLineValue(Row, CodedLineField(RowHeader, 'customer_name'), BufferVal);
                                    Journal."Description" := BufferVal; //customer_name
                                    CodedLineValue(Row, CodedLineField(RowHeader, 'payment_reference'), BufferVal);
                                    Journal."External Document No." := 'VP - ' + FORMAT(BufferVal); //payment_reference
                                    CodedLineValue(Row, CodedLineField(RowHeader, 'amount'), BufferVal);
                                    EVALUATE(BufferDec, BufferVal);
                                    Journal.VALIDATE(Amount, -1 * BufferDec); //amount

                                    // Flip the type based on the Amount Type:
                                    IF Journal.Amount > 0 THEN
                                        Journal."Document Type" := Journal."Document Type"::Refund
                                    ELSE
                                        Journal."Document Type" := Journal."Document Type"::Payment;

                                    CodedLineValue(Row, CodedLineField(RowHeader, 'invoice_number'), BufferVal);
                                    Journal."Applies-to Doc. No." := BufferVal;

                                    // Normally, I would look up the dimension, but because so much of the system is hardcoded to Shortcut 1 - they can't move it.
                                    // Will just use it.
                                    Journal."Shortcut Dimension 1 Code" := Customer."Global Dimension 1 Code";

                                    Journal.Insert;

                                    // Next, We need to render any FEES into the General Journal:
                                    CodedLineValue(Row, CodedLineField(RowHeader, 'payment_transaction_fee'), BufferVal);
                                    EVALUATE(BufferDec, BufferVal);
                                    IF BufferDec <> 0 THEN begin
                                        // Create a FeeJournal:
                                        FeeJournal.Init;

                                        // We'll Reuse the same document numbering type, but we don't want to unbalance it by putting them together.
                                        FeeJournal."Document No." := NoSeriesMgt.DoGetNextNo(VersaSetup."No. Series Code", WorkDate(), False, False);

                                        // In case they use the same batch/template for both - check and use the Payment one if so:
                                        IF (VersaSetup."Fees Journal Batch" = VersaSetup."Payments Journal Batch") AND (VersaSetup."Payments Journal Template" = VersaSetup."Fees Journal Template") THEN begin
                                            // Using Same Numbering, Use it:
                                            FeeJournal."Line No." := Next_PayLineNo;
                                            Next_PayLineNo := Next_PayLineNo + 1000;
                                        END ELSE BEGIN
                                            // Using A second Batch Numbering:
                                            FeeJournal."Line No." := Next_FeeLineNo;
                                            Next_FeeLineNo := Next_FeeLineNo + 1000;
                                        END;

                                        FeeJournal."Journal Template Name" := VersaSetup."Fees Journal Template";
                                        FeeJournal."Journal Batch Name" := VersaSetup."Fees Journal Batch";
                                        FeeJournal."Document Type" := FeeJournal."Document Type"::" ";
                                        FeeJournal."Account Type" := FeeJournal."Account Type"::"G/L Account";
                                        FeeJournal."Account No." := '453000';
                                        FeeJournal."Bal. Account Type" := Journal."Bal. Account Type"::"Bank Account";
                                        FeeJournal."Bal. Account No." := 'BA00055';
                                        FeeJournal.Description := 'ARC CC Fee - ' + Customer."No." + ' - ' + RowReference;

                                        // Copied Values:
                                        CodedLineValue(Row, CodedLineField(RowHeader, 'date'), BufferVal);
                                        FeeJournal."Posting Date" := BufferVal; //date
                                        CodedLineValue(Row, CodedLineField(RowHeader, 'payment_reference'), BufferVal);
                                        FeeJournal."External Document No." := 'VP - ' + FORMAT(BufferVal); //payment_reference
                                        FeeJournal.Validate(Amount, -1 * BufferDec); // From Above (Calculated to get here)

                                        FeeJournal."Shortcut Dimension 1 Code" := Customer."Global Dimension 1 Code";

                                        FeeJournal.Insert;
                                    end;

                                    LineNo := LineNo + 1;
                                END ELSE BEGIN
                                    // Then this is a skip value.
                                end;
                            end;
                        end;

                        Success := true;
                    end;
            END;

            // Close the file to end the processing
            InFile.CLOSE;

            // Backup the file if there was success (or delete if backup is off)
            BackupFile(FileName);
        end;
    end;

    // Output files are coded by the incoming file (to leave room for future expansion)
    procedure OutputFile(FileType: Text)
    var
        VersaSetup: Record "VersaPay Setup";
        OutFilename: Text;
        CustLedger: Record "Cust. Ledger Entry";
        SalesInvoiceHeader: Record "Sales Invoice Header";
        SalesInvoiceLine: Record "Sales Invoice Line";
        CreditHeader: Record "Sales Cr.Memo Header";
        CreditLine: Record "Sales Cr.Memo Line";
        Customer: Record "Customer";
        CustomerEmail: Text; // This will store the first email found to avoid pulling it twice.
        PaymentTermText: Text; // This is for modifying payment terms days as specified in the Instructions.
        CustomerNo: Code[20]; // Storage for the customer to use during sub-pulls (Like when using Bill-To instead)
        CustomRepSel: Record "Custom Report Selection"; // Mislabeled as 'Customer Report Selection' on the design...
        CSVBuffer: Record "CSV Buffer" temporary; // Buffer is not great for input, but not bad for output
        LineNo: Integer;
    begin
        // Make sure the setup exists:
        IF NOT VersaSetup.GET THEN ERROR('Could not get VersaPay Setup');

        // Lets Process based on the type of File:
        CASE FileType of
            'CUSTOMER':
                begin
                    // Make sure an Out Folder and Delimeter are set:
                    IF NOT ((STRLEN(VersaSetup."Out Folder") > 0) OR NOT (STRLEN(VersaSetup."File Delimeter") > 0)) THEN ERROR('Output Files require an Out Folder and File Delimeter to generate output files.');

                    // Generate a Filename:
                    OutFilename := VersaSetup."Out Folder" + '/customer' + FORMAT(CURRENTDATETIME(), 0, '<Year4><Month,2><Day,2><Hours24><Minutes,2><Seconds,2>') + '.csv';

                    // Prepare the CSV Buffer:
                    CSVBuffer.DeleteAll;
                    LineNo := 1;

                    // Add a Header Line:
                    CSVBuffer.InsertEntry(LineNo, 1, 'identifier');
                    CSVBuffer.InsertEntry(LineNo, 2, 'parent_identifier');
                    CSVBuffer.InsertEntry(LineNo, 3, 'name');
                    CSVBuffer.InsertEntry(LineNo, 4, 'credit_limit_cents');
                    CSVBuffer.InsertEntry(LineNo, 5, 'terms_value');
                    CSVBuffer.InsertEntry(LineNo, 6, 'terms_type');
                    CSVBuffer.InsertEntry(LineNo, 7, 'contact_email');
                    CSVBuffer.InsertEntry(LineNo, 8, 'CC_Contact_Email');
                    CSVBuffer.InsertEntry(LineNo, 9, 'telephone');
                    CSVBuffer.InsertEntry(LineNo, 10, 'Fax');
                    CSVBuffer.InsertEntry(LineNo, 11, 'address_1');
                    CSVBuffer.InsertEntry(LineNo, 12, 'address_2');
                    CSVBuffer.InsertEntry(LineNo, 13, 'city');
                    CSVBuffer.InsertEntry(LineNo, 14, 'postal_code');
                    CSVBuffer.InsertEntry(LineNo, 15, 'province');
                    CSVBuffer.InsertEntry(LineNo, 16, 'country');
                    CSVBuffer.InsertEntry(LineNo, 17, 'tags');
                    // Increment Line
                    LineNo := LineNo + 1;

                    // Write the File:
                    //CSVBuffer.SaveData(OutFileName, VersaSetup."File Delimeter");

                    // This will roll the Customer list, and for each customer grab and export the Email lines.
                    Customer.Reset;
                    Customer.SETFILTER("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    Customer.SETFILTER("No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    Customer.SETFILTER("Customer Posting Group", '<>JAPAN');
                    IF Customer.FindSet THEN
                        REPEAT
                            // Let's Prepare our Export, Adding Fields to this line:
                            CSVBuffer.InsertEntry(LineNo, 1, strprep(Customer."No.")); // 'identifier'
                            CSVBuffer.InsertEntry(LineNo, 2, strprep(Customer."Bill-to Customer No.")); // 'parent_identifier'
                            CSVBuffer.InsertEntry(LineNo, 3, strprep(Customer.Name)); // 'name'
                            CSVBuffer.InsertEntry(LineNo, 4, strprep(Customer."Credit Limit (LCY)")); // 'credit_limit_cents'

                            // Payment terms SHOULD be a sub-table, but instead they pulled the 'Code' value and strip the leading N if it exists.  Seriously I couldn't make this up.
                            PaymentTermText := Customer."Payment Terms Code";
                            IF COPYSTR(PaymentTermText, 1, 1) = 'N' THEN PaymentTermText := COPYSTR(PaymentTermText, 2);
                            CSVBuffer.InsertEntry(LineNo, 5, strprep(PaymentTermText)); // 'terms_value' - but with the N stripped off...
                            CSVBuffer.InsertEntry(LineNo, 6, 'Day'); // 'terms_type'

                            // To get the Email Lines, we need to look for TWO kinds of Email.  First, we look for the Invoice and use it if found:
                            // Note: We COULD switch to the Bill-to Customer for this, but since the Jet isn't doing that, I'm not going to yet.
                            // 7: First, use the first Invoicing or Statement one if found:
                            CustomerEmail := '';
                            CustomRepSel.Reset();
                            CustomRepSel.SETRANGE("Source Type", 18);
                            CustomRepSel.SETRANGE("Source No.", Customer."No.");
                            CustomRepSel.SETRANGE("Usage", CustomRepSel."Usage"::"S.Invoice"); // Select for Invoice
                            IF CustomRepSel.FINDSET THEN BEGIN // Customer Type (Table 18)
                                CSVBuffer.InsertEntry(LineNo, 7, strprep(CustomRepSel."Send To Email")); // 'contact_email'
                                CustomerEmail := CustomRepSel."Send To Email";
                            END ELSE BEGIN
                                // IF it's NOT found, we attempt to use the STATEMENT email:
                                CustomRepSel.Reset();
                                CustomRepSel.SETRANGE("Source Type", 18);
                                CustomRepSel.SETRANGE("Source No.", Customer."No.");
                                CustomRepSel.SETRANGE("Usage", CustomRepSel."Usage"::"C.Statement"); // Select for Statement
                                IF CustomRepSel.FINDSET THEN BEGIN // Customer Type (Table 18)
                                    // Use Statement if found:
                                    CSVBuffer.InsertEntry(LineNo, 7, strprep(CustomRepSel."Send To Email")); // 'contact_email'
                                    CustomerEmail := CustomRepSel."Send To Email";
                                END ELSE BEGIN
                                    // Nothing Found.  Add Empty Field:
                                    CSVBuffer.InsertEntry(LineNo, 7, ''); // 'contact_email'     
                                END;
                            END;
                            // 8: Use the NEXT Invoice or Statement that doesn't match the first one, if we have a first one.
                            IF STRLEN(CustomerEmail) > 0 THEN BEGIN
                                CustomRepSel.Reset();
                                CustomRepSel.SETRANGE("Source Type", 18);
                                CustomRepSel.SETRANGE("Source No.", Customer."No.");
                                CustomRepSel.SETRANGE("Usage", CustomRepSel."Usage"::"S.Invoice"); // Select for Invoice
                                CustomRepSel.SETFILTER("Send To Email", '<>' + CustomerEmail);
                                IF CustomRepSel.FINDSET THEN BEGIN // Customer Type (Table 18)
                                    CSVBuffer.InsertEntry(LineNo, 8, strprep(CustomRepSel."Send To Email")); // 'contact_email'
                                END ELSE BEGIN
                                    // IF it's NOT found, we attempt to use the STATEMENT email:
                                    CustomRepSel.Reset();
                                    CustomRepSel.SETRANGE("Source Type", 18);
                                    CustomRepSel.SETRANGE("Source No.", Customer."No.");
                                    CustomRepSel.SetRange("Usage", CustomRepSel."Usage"::"C.Statement"); // Select for Statement
                                    CustomRepSel.SETFILTER("Send To Email", '<>' + CustomerEmail);
                                    IF CustomRepSel.FINDSET THEN BEGIN // Customer Type (Table 18)
                                        // Use Statement if found:
                                        CSVBuffer.InsertEntry(LineNo, 8, strprep(CustomRepSel."Send To Email")); // 'contact_email'
                                    END ELSE BEGIN
                                        // Nothing Found.  Add Empty Field:
                                        CSVBuffer.InsertEntry(LineNo, 8, ''); // 'contact_email'     
                                    END;
                                END;
                            END ELSE
                                CSVBuffer.InsertEntry(LineNo, 8, ''); // 'contact_email' 

                            CSVBuffer.InsertEntry(LineNo, 9, strprep(Customer."Phone No.")); // 'telephone'
                            CSVBuffer.InsertEntry(LineNo, 10, strprep(Customer."Fax No.")); // 'Fax'
                            CSVBuffer.InsertEntry(LineNo, 11, strprep(Customer.Address)); // 'address_1'
                            CSVBuffer.InsertEntry(LineNo, 12, strprep(Customer."Address 2")); // 'address_2'
                            CSVBuffer.InsertEntry(LineNo, 13, strprep(Customer.City)); // 'city'
                            CSVBuffer.InsertEntry(LineNo, 14, strprep(Customer."Post Code")); // 'postal_code'
                            CSVBuffer.InsertEntry(LineNo, 15, strprep(Customer.County)); // (State) 'province'
                            CSVBuffer.InsertEntry(LineNo, 16, strprep(Customer."Country/Region Code")); // 'country'
                            CSVBuffer.InsertEntry(LineNo, 17, strprep(CompanyName + ';' + Customer."Global Dimension 1 Code" + ';' + Customer."Gen. Bus. Posting Group")); // 'tags'

                            // Increment Line
                            LineNo := LineNo + 1;
                        UNTIL Customer.Next = 0;

                    // Write the File:
                    CSVBuffer.SaveData(OutFileName, VersaSetup."File Delimeter");
                end;
            'INVOICE':
                begin
                    // Make sure an Out Folder and Delimeter are set:
                    IF NOT ((STRLEN(VersaSetup."Out Folder") > 0) OR NOT (STRLEN(VersaSetup."File Delimeter") > 0)) THEN ERROR('Output Files require an Out Folder and File Delimeter to generate output files.');

                    // Generate a Filename:
                    OutFilename := VersaSetup."Out Folder" + '/invoice' + FORMAT(CURRENTDATETIME(), 0, '<Year4><Month,2><Day,2><Hours24><Minutes,2><Seconds,2>') + '.csv';

                    // Prepare the CSV Buffer:
                    CSVBuffer.DeleteAll;
                    LineNo := 1;

                    // Add a Header Line:
                    CSVBuffer.InsertEntry(LineNo, 1, 'number');
                    CSVBuffer.InsertEntry(LineNo, 2, 'item_code');
                    CSVBuffer.InsertEntry(LineNo, 3, 'line_item_number');
                    CSVBuffer.InsertEntry(LineNo, 4, 'line_item_description');
                    CSVBuffer.InsertEntry(LineNo, 5, 'line_item_quantity');
                    CSVBuffer.InsertEntry(LineNo, 6, 'line_item_unit_cost');
                    CSVBuffer.InsertEntry(LineNo, 7, 'line_item_amount');
                    CSVBuffer.InsertEntry(LineNo, 8, 'line_item_discount_amt');
                    CSVBuffer.InsertEntry(LineNo, 9, 'line_item_total_amount');
                    CSVBuffer.InsertEntry(LineNo, 10, 'subtotal');
                    CSVBuffer.InsertEntry(LineNo, 11, 'tax');
                    CSVBuffer.InsertEntry(LineNo, 12, 'amount');
                    CSVBuffer.InsertEntry(LineNo, 13, 'balance');
                    CSVBuffer.InsertEntry(LineNo, 14, 'shipment_date');
                    CSVBuffer.InsertEntry(LineNo, 15, 'purchase_order_number');
                    CSVBuffer.InsertEntry(LineNo, 16, 'sales_order_number');
                    CSVBuffer.InsertEntry(LineNo, 17, 'customer_identifier');
                    CSVBuffer.InsertEntry(LineNo, 18, 'division');
                    CSVBuffer.InsertEntry(LineNo, 19, 'date');
                    CSVBuffer.InsertEntry(LineNo, 20, 'order_date');
                    CSVBuffer.InsertEntry(LineNo, 21, 'due_date');
                    CSVBuffer.InsertEntry(LineNo, 22, 'shipping_name');
                    CSVBuffer.InsertEntry(LineNo, 23, 'shipping_address_1');
                    CSVBuffer.InsertEntry(LineNo, 24, 'shipping_address_2');
                    CSVBuffer.InsertEntry(LineNo, 25, 'shipping_city');
                    CSVBuffer.InsertEntry(LineNo, 26, 'shipping_postal_code');
                    CSVBuffer.InsertEntry(LineNo, 27, 'shipping_province');
                    CSVBuffer.InsertEntry(LineNo, 28, 'shipping_country');
                    CSVBuffer.InsertEntry(LineNo, 29, 'terms');
                    CSVBuffer.InsertEntry(LineNo, 30, 'rep');
                    CSVBuffer.InsertEntry(LineNo, 31, 'via');
                    CSVBuffer.InsertEntry(LineNo, 32, 'your_reference');
                    CSVBuffer.InsertEntry(LineNo, 33, 'finance_charge');
                    CSVBuffer.InsertEntry(LineNo, 34, 'nsf_fee');
                    CSVBuffer.InsertEntry(LineNo, 35, 'convenience_fee');

                    // Increment Line
                    LineNo := LineNo + 1;

                    // NOW THE INSANITY.
                    // Given that the document number is reused in the customer ledger entry, we'll likely need to do a 'total' to get balance (and not a first)
                    // AND that we're supposed to filter out be remaining - we'll use the ledger entry to lead what should be copied to avoid the issues in the file.
                    // This will also let us cycle ONCE to get the 'AR Beg Bal' lines as well - since they appear to use the same limits.
                    // Part -1: The Dimensions aren't flowing right, so we have to do this by Customer first:
                    Customer.Reset;
                    Customer.SETFILTER("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    Customer.SETFILTER("No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    Customer.SETFILTER("Customer Posting Group", '<>JAPAN');
                    IF Customer.FindSet THEN
                        REPEAT
                            // Part 1: Invoices
                            SalesInvoiceHeader.Reset;
                            //SalesInvoiceHeader.SETFILTER("Shortcut Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                            SalesInvoiceHeader.SETFILTER("Remaining Amount", '<>0');
                            SalesInvoiceHeader.SETFILTER("Sell-To Customer No.", Customer."No.");
                            SalesInvoiceHeader.SETFILTER("Customer Posting Group", '<>JAPAN');
                            IF SalesInvoiceHeader.FindSet THEN
                                REPEAT
                                    SalesInvoiceLine.RESET;
                                    SalesInvoiceLine.SetFilter(Amount, '<>0');//RPS 05/22/2023 - 
                                    SalesInvoiceLine.SETFILTER("Document No.", SalesInvoiceHeader."No.");
                                    IF SalesInvoiceLine.FindSet THEN
                                        REPEAT
                                            // We should have everything they need now:
                                            CSVBuffer.InsertEntry(LineNo, 1, strprep(SalesInvoiceLine."Document No."));  //number
                                            CSVBuffer.InsertEntry(LineNo, 2, strprep(SalesInvoiceLine."No."));  //item_code
                                            CSVBuffer.InsertEntry(LineNo, 3, strprep(SalesInvoiceLine."Line No."));  //line_item_number
                                            CSVBuffer.InsertEntry(LineNo, 4, strprep(SalesInvoiceLine."Description"));  //line_item_description
                                            CSVBuffer.InsertEntry(LineNo, 5, strprep(SalesInvoiceLine."Quantity"));  //line_item_quantity
                                            CSVBuffer.InsertEntry(LineNo, 6, strprep(SalesInvoiceLine."Unit Price"));  //line_item_unit_cost
                                            CSVBuffer.InsertEntry(LineNo, 7, strprep(SalesInvoiceLine."Unit Price" * SalesInvoiceLIne.Quantity));  //line_item_amount
                                            CSVBuffer.InsertEntry(LineNo, 8, strprep(SalesInvoiceLine."Line Discount Amount"));  //line_item_discount_amt
                                            CSVBuffer.InsertEntry(LineNo, 9, strprep(SalesInvoiceLine."Amount"));  //line_item_total_amount
                                            SalesInvoiceHeader.CalcFields("Amount", "Amount Including VAT", "Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                                            CSVBuffer.InsertEntry(LineNo, 10, strprep(SalesInvoiceHeader."Amount"));  //subtotal
                                            CSVBuffer.InsertEntry(LineNo, 11, strprep(SalesInvoiceHeader."Amount Including VAT" - SalesInvoiceHeader."Amount"));  //tax (Difference between these two?)
                                            CSVBuffer.InsertEntry(LineNo, 12, strprep(SalesInvoiceHeader."Amount Including VAT"));  //amount
                                            CSVBuffer.InsertEntry(LineNo, 13, strprep(SalesInvoiceHeader."Remaining Amount"));  //balance
                                            CSVBuffer.InsertEntry(LineNo, 14, strprep(SalesInvoiceLine."Shipment Date"));  //shipment_date
                                            CSVBuffer.InsertEntry(LineNo, 15, strprep(SalesInvoiceHeader."External Document No."));  //purchase_order_number
                                            CSVBuffer.InsertEntry(LineNo, 16, strprep(SalesInvoiceLine."Order No."));  //sales_order_number
                                            CSVBuffer.InsertEntry(LineNo, 17, strprep(SalesInvoiceHeader."Bill-To Customer No."));  //customer_identifier
                                            CSVBuffer.InsertEntry(LineNo, 18, strprep(CompanyName));  //division (Setting to Company)
                                            CSVBuffer.InsertEntry(LineNo, 19, strprep(SalesInvoiceLine."Posting Date"));  //date
                                            CSVBuffer.InsertEntry(LineNo, 20, strprep(SalesInvoiceHeader."Order Date"));  //order_date
                                            CSVBuffer.InsertEntry(LineNo, 21, strprep(SalesInvoiceHeader."Due Date"));  //due_date
                                            CSVBuffer.InsertEntry(LineNo, 22, strprep(SalesInvoiceHeader."Ship-to Name"));  //shipping_name
                                            CSVBuffer.InsertEntry(LineNo, 23, strprep(SalesInvoiceHeader."Ship-to Address"));  //shipping_address_1
                                            CSVBuffer.InsertEntry(LineNo, 24, strprep(SalesInvoiceHeader."Ship-to Address 2"));  //shipping_address_2
                                            CSVBuffer.InsertEntry(LineNo, 25, strprep(SalesInvoiceHeader."Ship-to City"));  //shipping_city
                                            CSVBuffer.InsertEntry(LineNo, 26, strprep(SalesInvoiceHeader."Ship-to Post Code"));  //shipping_postal_code
                                            CSVBuffer.InsertEntry(LineNo, 27, strprep(SalesInvoiceHeader."Ship-to County"));  //shipping_province
                                            CSVBuffer.InsertEntry(LineNo, 28, strprep(SalesInvoiceHeader."Ship-to Country/Region Code"));  //shipping_country
                                            CSVBuffer.InsertEntry(LineNo, 29, strprep(SalesInvoiceHeader."Payment Terms Code"));  //terms
                                            CSVBuffer.InsertEntry(LineNo, 30, strprep(SalesInvoiceHeader."Salesperson Code"));  //rep
                                            CSVBuffer.InsertEntry(LineNo, 31, strprep(SalesInvoiceHeader."Shipping Agent Code"));  //via
                                            CSVBuffer.InsertEntry(LineNo, 32, strprep(SalesInvoiceHeader."Your Reference"));  //your_reference
                                            CSVBuffer.InsertEntry(LineNo, 33, '0.01');  //finance_charge
                                            CSVBuffer.InsertEntry(LineNo, 34, '45');  //nsf_fee
                                            CSVBuffer.InsertEntry(LineNo, 35, '2.5');  //convenience_fee

                                            // Increment Line
                                            LineNo := LineNo + 1;

                                        UNTIL SalesInvoiceLine.Next = 0;
                                UNTIL SalesInvoiceHeader.Next = 0;
                            // Part 2: Credits
                            CreditHeader.Reset;
                            //CreditHeader.SETFILTER("Shortcut Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                            CreditHeader.SETFILTER("Remaining Amount", '<>0');
                            CreditHeader.SETFILTER("Sell-To Customer No.", Customer."No.");
                            CreditHeader.SETFILTER("Customer Posting Group", '<>JAPAN');
                            IF CreditHeader.FindSet THEN
                                REPEAT
                                    CreditLine.RESET;
                                    CreditLine.SetFilter(Amount, '<>0');
                                    CreditLine.SETFILTER("Document No.", CreditHeader."No.");
                                    IF CreditLine.FindSet THEN
                                        REPEAT
                                            // We should have everything they need now:
                                            CSVBuffer.InsertEntry(LineNo, 1, strprep(CreditLine."Document No."));  //number
                                            CSVBuffer.InsertEntry(LineNo, 2, strprep(CreditLine."No."));  //item_code
                                            CSVBuffer.InsertEntry(LineNo, 3, strprep(CreditLine."Line No."));  //line_item_number
                                            CSVBuffer.InsertEntry(LineNo, 4, strprep(CreditLine."Description"));  //line_item_description
                                            CSVBuffer.InsertEntry(LineNo, 5, strprep(CreditLine."Quantity"));  //line_item_quantity
                                            CSVBuffer.InsertEntry(LineNo, 6, strprep(-1 * CreditLine."Unit Price"));  //line_item_unit_cost
                                            CSVBuffer.InsertEntry(LineNo, 7, strprep(-1 * (CreditLine."Unit Price" * CreditLine."Quantity")));  //line_item_amount
                                            CSVBuffer.InsertEntry(LineNo, 8, strprep(CreditLine."Line Discount Amount"));  //line_item_discount_amt
                                            CSVBuffer.InsertEntry(LineNo, 9, strprep(-1 * CreditLine."Amount"));  //line_item_total_amount
                                            CreditHeader.CalcFields("Amount", "Amount Including VAT", "Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                                            CSVBuffer.InsertEntry(LineNo, 10, strprep(-1 * CreditHeader."Amount"));  //subtotal
                                            CSVBuffer.InsertEntry(LineNo, 11, strprep(-1 * (CreditHeader."Amount Including VAT" - CreditHeader."Amount")));  //tax (Difference between these two?)
                                            CSVBuffer.InsertEntry(LineNo, 12, strprep(-1 * CreditHeader."Amount Including VAT"));  //amount
                                            CSVBuffer.InsertEntry(LineNo, 13, strprep(CreditHeader."Remaining Amount"));  //balance
                                            CSVBuffer.InsertEntry(LineNo, 14, strprep(CreditLine."Shipment Date"));  //shipment_date
                                            CSVBuffer.InsertEntry(LineNo, 15, strprep(CreditHeader."External Document No."));  //purchase_order_number
                                            CSVBuffer.InsertEntry(LineNo, 16, '');  //sales_order_number (Blank)
                                            CSVBuffer.InsertEntry(LineNo, 17, strprep(CreditHeader."Bill-To Customer No."));  //customer_identifier
                                            CSVBuffer.InsertEntry(LineNo, 18, strprep(CompanyName));  //division (Setting to Company)
                                            CSVBuffer.InsertEntry(LineNo, 19, strprep(CreditLine."Posting Date"));  //date
                                            CSVBuffer.InsertEntry(LineNo, 20, '');  //order_date (Blank)
                                            CSVBuffer.InsertEntry(LineNo, 21, strprep(CreditHeader."Due Date"));  //due_date
                                            CSVBuffer.InsertEntry(LineNo, 22, strprep(CreditHeader."Ship-to Name"));  //shipping_name
                                            CSVBuffer.InsertEntry(LineNo, 23, strprep(CreditHeader."Ship-to Address"));  //shipping_address_1
                                            CSVBuffer.InsertEntry(LineNo, 24, strprep(CreditHeader."Ship-to Address 2"));  //shipping_address_2
                                            CSVBuffer.InsertEntry(LineNo, 25, strprep(CreditHeader."Ship-to City"));  //shipping_city
                                            CSVBuffer.InsertEntry(LineNo, 26, strprep(CreditHeader."Ship-to Post Code"));  //shipping_postal_code
                                            CSVBuffer.InsertEntry(LineNo, 27, strprep(CreditHeader."Ship-to County"));  //shipping_province
                                            CSVBuffer.InsertEntry(LineNo, 28, strprep(CreditHeader."Ship-to Country/Region Code"));  //shipping_country
                                            CSVBuffer.InsertEntry(LineNo, 29, strprep(CreditHeader."Payment Terms Code"));  //terms
                                            CSVBuffer.InsertEntry(LineNo, 30, strprep(CreditHeader."Salesperson Code"));  //rep
                                            CSVBuffer.InsertEntry(LineNo, 31, '');  //via (Blank)
                                            CSVBuffer.InsertEntry(LineNo, 32, strprep(CreditHeader."Your Reference"));  //your_reference
                                            CSVBuffer.InsertEntry(LineNo, 33, '');  //finance_charge (Blank Credit only?)
                                            CSVBuffer.InsertEntry(LineNo, 34, '');  //nsf_fee (Blank Credit only?)
                                            CSVBuffer.InsertEntry(LineNo, 35, '');  //convenience_fee (Blank Credit only?)

                                            // Increment Line
                                            LineNo := LineNo + 1;

                                        UNTIL CreditLine.Next = 0;
                                UNTIL CreditHeader.Next = 0;

                            // Part 3: ... Balances?  Ledger entries?  I'm unsure.
                            CustLedger.Reset;
                            //CustLedger.SETFILTER("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                            CustLedger.SETFILTER("Remaining Amount", '<>0');
                            CustLedger.SETFILTER("Document Type", '<>Refund'); // Requested by Jessica to offset a single Beg Bal that has the 'wrong type' 4/20/2023 - reflexive change in the payment file below.
                            CustLedger.SETFILTER("Description", 'AR BEG BALANCE');
                            CustLedger.SETFILTER("Sell-To Customer No.", Customer."No.");
                            CustLedger.SETFILTER("Customer Posting Group", '<>JAPAN');
                            IF CustLedger.FindSet THEN
                                REPEAT
                                    CSVBuffer.InsertEntry(LineNo, 1, strprep(CustLedger."Document No."));  // 'number'
                                    CSVBuffer.InsertEntry(LineNo, 2, strprep(CustLedger."Document Type"));  // 'item_code'
                                    CSVBuffer.InsertEntry(LineNo, 3, '');  // 'line_item_number'
                                    CSVBuffer.InsertEntry(LineNo, 4, strprep(CustLedger."Description"));  // 'line_item_description'
                                    CSVBuffer.InsertEntry(LineNo, 5, '1');  // 'line_item_quantity'
                                    CSVBuffer.InsertEntry(LineNo, 6, '0');  // 'line_item_unit_cost'
                                    CustLedger.CalcFields("Amount", "Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                                    CSVBuffer.InsertEntry(LineNo, 7, strprep(CustLedger."Amount"));  // 'line_item_amount'
                                    CSVBuffer.InsertEntry(LineNo, 8, '0');  // 'line_item_discount_amt'
                                    CSVBuffer.InsertEntry(LineNo, 9, strprep(CustLedger."Amount"));  // 'line_item_total_amount'
                                    CSVBuffer.InsertEntry(LineNo, 10, strprep(CustLedger."Amount"));  // 'subtotal'
                                    CSVBuffer.InsertEntry(LineNo, 11, '0');  // 'tax'
                                    CSVBuffer.InsertEntry(LineNo, 12, strprep(CustLedger."Amount"));  // 'amount'
                                    CSVBuffer.InsertEntry(LineNo, 13, strprep(CustLedger."Remaining Amount"));  // 'balance'
                                    CSVBuffer.InsertEntry(LineNo, 14, '');  // 'shipment_date'
                                    CSVBuffer.InsertEntry(LineNo, 15, '');  // 'purchase_order_number'
                                    CSVBuffer.InsertEntry(LineNo, 16, '');  // 'sales_order_number'
                                    CSVBuffer.InsertEntry(LineNo, 17, strprep(CustLedger."Sell-To Customer No."));  // 'customer_identifier'
                                    CSVBuffer.InsertEntry(LineNo, 18, strprep(CompanyName));  // 'division'
                                    CSVBuffer.InsertEntry(LineNo, 19, strprep(CustLedger."Posting Date"));  // 'date'
                                    CSVBuffer.InsertEntry(LineNo, 20, '');  // 'order_date'
                                    CSVBuffer.InsertEntry(LineNo, 21, strprep(CustLedger."Due Date"));  // 'due_date'
                                    CSVBuffer.InsertEntry(LineNo, 22, strprep(Customer."Name"));  // 'shipping_name'
                                    CSVBuffer.InsertEntry(LineNo, 23, strprep(Customer."Address"));  // 'shipping_address_1'
                                    CSVBuffer.InsertEntry(LineNo, 24, strprep(Customer."Address 2"));  // 'shipping_address_2'
                                    CSVBuffer.InsertEntry(LineNo, 25, strprep(Customer."City"));  // 'shipping_city'
                                    CSVBuffer.InsertEntry(LineNo, 26, strprep(Customer."Post Code"));  // 'shipping_postal_code'
                                    CSVBuffer.InsertEntry(LineNo, 27, strprep(Customer."County"));  // 'shipping_province'
                                    CSVBuffer.InsertEntry(LineNo, 28, strprep(Customer."Country/Region Code"));  // 'shipping_country'
                                    CSVBuffer.InsertEntry(LineNo, 29, strprep(Customer."Payment Terms Code"));  // 'terms'
                                    CSVBuffer.InsertEntry(LineNo, 30, strprep(Customer."Salesperson Code"));  // 'rep'
                                    CSVBuffer.InsertEntry(LineNo, 31, strprep(Customer."Shipping Agent Code"));  // 'via'
                                    CSVBuffer.InsertEntry(LineNo, 32, '');  // 'your_reference' This is not set in any of the records, so ignoring
                                    CSVBuffer.InsertEntry(LineNo, 33, '0.01');  //finance_charge
                                    CSVBuffer.InsertEntry(LineNo, 34, '45');  //nsf_fee
                                    CSVBuffer.InsertEntry(LineNo, 35, '2.5');  //convenience_fee

                                    // Increment Line
                                    LineNo := LineNo + 1;
                                UNTIL CustLedger.Next = 0;
                        UNTIL Customer.Next = 0;

                    // Write the File:
                    CSVBuffer.SaveData(OutFileName, VersaSetup."File Delimeter");
                end;
            /*'PAYMENT_OUT'://>>RPS-05182023  Commenting Out
                begin
                    // Make sure an Out Folder and Delimeter are set:
                    IF NOT ((STRLEN(VersaSetup."Out Folder") > 0) OR NOT (STRLEN(VersaSetup."File Delimeter") > 0)) THEN ERROR('Output Files require an Out Folder and File Delimeter to generate output files.');

                    // Generate a Filename:
                    OutFilename := VersaSetup."Out Folder" + '/paymentbc' + FORMAT(CURRENTDATETIME(), 0, '<Year4><Month,2><Day,2><Hours24><Minutes,2><Seconds,2>') + '.csv';

                    // Prepare the CSV Buffer:
                    CSVBuffer.DeleteAll;
                    LineNo := 1;

                    // Add a header line:
                    CSVBuffer.InsertEntry(LineNo, 1, 'identifier');
                    CSVBuffer.InsertEntry(LineNo, 2, 'date');
                    CSVBuffer.InsertEntry(LineNo, 3, 'currency');
                    CSVBuffer.InsertEntry(LineNo, 4, 'customer_identifier');
                    CSVBuffer.InsertEntry(LineNo, 5, 'customer_name');
                    CSVBuffer.InsertEntry(LineNo, 6, 'payment_note');
                    CSVBuffer.InsertEntry(LineNo, 7, 'payment_total');

                    // Increment Line
                    LineNo := LineNo + 1;

                    Customer.Reset;
                    Customer.SETFILTER("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    Customer.SETFILTER("No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    Customer.SETFILTER("Customer Posting Group", '<>JAPAN');
                    IF Customer.FindSet THEN
                        REPEAT
                            CustLedger.Reset;
                            //CustLedger.SETFILTER("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                            CustLedger.SETFILTER("Remaining Amount", '<>0');
                            // I guess a payment here is anything NOT sent above?... twice?
                            CustLedger.SETFILTER("Document Type", '<>Credit Memo&<>Reminder&<>Finance Charge Memo&<>Invoice');
                            //CustLedger.SETFILTER("Description", '<>AR BEG BALANCE');
                            // No move this single item out of alignment per Jessica, we will have to scan all and allow refunds back in.
                            CustLedger.SETFILTER("Customer No.", Customer."No.");
                            CustLedger.SETFILTER("Customer Posting Group", '<>JAPAN');
                            IF CustLedger.FindSet THEN
                                REPEAT

                                    // Hide any BEG BALANCE except Refund (Per Jessica 4/20/2023):
                                    IF ((UpperCase(CustLedger.Description) <> 'AR BEG BALANCE') OR ((CustLedger."Document Type" = CustLedger."Document Type"::Refund) AND (UpperCase(CustLedger.Description) = 'AR BEG BALANCE'))) THEN begin
                                        CSVBuffer.InsertEntry(LineNo, 1, strprep(CustLedger."Document No." + ' - ' + FORMAT(CustLedger."Entry No."))); // identifier
                                        CSVBuffer.InsertEntry(LineNo, 2, strprep(CustLedger."Posting Date")); // date
                                        CSVBuffer.InsertEntry(LineNo, 3, 'USD'); // currency
                                        CSVBuffer.InsertEntry(LineNo, 4, strprep(CustLedger."Customer No.")); // customer_identifier
                                        CSVBuffer.InsertEntry(LineNo, 5, strprep(Customer."Name")); // customer_name
                                        CSVBuffer.InsertEntry(LineNo, 6, strprep(CompanyName + ' DocType:' + FORMAT(CustLedger."Document Type") + ' Description:' + CustLedger.Description)); // payment_note
                                        CustLedger.CalcFields("Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                                        CSVBuffer.InsertEntry(LineNo, 7, strprep(-1 * CustLedger."Remaining Amount")); // payment_total

                                        // Increment Line
                                        LineNo := LineNo + 1;
                                    END;
                                UNTIL CustLedger.Next = 0;
                        UNTIL Customer.Next = 0;

                    // Write the File:
                    CSVBuffer.SaveData(OutFileName, VersaSetup."File Delimeter");
                end;
                *///<<RPS-05182023  Commenting Out
            'RECON':
                begin
                    // Make sure an Out Folder and Delimeter are set:
                    IF NOT ((STRLEN(VersaSetup."Out Folder") > 0) OR NOT (STRLEN(VersaSetup."File Delimeter") > 0)) THEN ERROR('Output Files require an Out Folder and File Delimeter to generate output files.');

                    // Generate a Filename:
                    OutFilename := VersaSetup."Out Folder" + '/recon' + FORMAT(CURRENTDATETIME(), 0, '<Year4><Month,2><Day,2><Hours24><Minutes,2><Seconds,2>') + '.csv';

                    // Prepare the CSV Buffer:
                    CSVBuffer.DeleteAll;
                    LineNo := 1;

                    // Add a header line:
                    CSVBuffer.InsertEntry(LineNo, 1, 'reconciliation_invoice_number');
                    CSVBuffer.InsertEntry(LineNo, 2, 'balance');
                    CSVBuffer.InsertEntry(LineNo, 3, 'division');

                    // Increment Line
                    LineNo := LineNo + 1;

                    Customer.Reset;
                    Customer.SETFILTER("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    Customer.SETFILTER("No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    Customer.SETFILTER("Customer Posting Group", '<>JAPAN');
                    IF Customer.FindSet THEN
                        REPEAT
                            CustLedger.Reset;
                            //CustLedger.SETFILTER("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                            CustLedger.SETFILTER("Remaining Amount", '<>0');
                            // I guess a payment here is anything NOT sent above?... twice?
                            // Removing this filter temporarily
                            CustLedger.SETFILTER("Document Type", 'Credit Memo|Reminder|Finance Charge Memo|Invoice');
                            CustLedger.SETFILTER("Customer No.", Customer."No.");
                            CustLedger.SETFILTER("Customer Posting Group", '<>JAPAN');
                            IF CustLedger.FindSet THEN
                                REPEAT
                                    CSVBuffer.InsertEntry(LineNo, 1, strprep(CustLedger."Document No.")); // 'reconciliation_invoice_number'
                                    CustLedger.CalcFields("Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                                    CSVBuffer.InsertEntry(LineNo, 2, strprep(CustLedger."Remaining Amount")); // 'balance'
                                    CSVBuffer.InsertEntry(LineNo, 3, strprep(CompanyName)); // 'division'

                                    // Increment Line
                                    LineNo := LineNo + 1;
                                UNTIL CustLedger.Next = 0;
                        UNTIL Customer.Next = 0;

                    // Write the File:
                    CSVBuffer.SaveData(OutFileName, VersaSetup."File Delimeter");
                end;
        END;
    end;

    // Output files are coded by the incoming file (to leave room for future expansion)
    // This is the version that uses the Dimensions from the records, not the customer only.
    procedure OutputFile_DIRECT(FileType: Text)
    var
        VersaSetup: Record "VersaPay Setup";
        OutFilename: Text;
        CustLedger: Record "Cust. Ledger Entry";
        SalesInvoiceHeader: Record "Sales Invoice Header";
        SalesInvoiceLine: Record "Sales Invoice Line";
        CreditHeader: Record "Sales Cr.Memo Header";
        CreditLine: Record "Sales Cr.Memo Line";
        Customer: Record "Customer";
        CustomerEmail: Text; // This will store the first email found to avoid pulling it twice.
        PaymentTermText: Text; // This is for modifying payment terms days as specified in the Instructions.
        CustomerNo: Code[20]; // Storage for the customer to use during sub-pulls (Like when using Bill-To instead)
        CustomRepSel: Record "Custom Report Selection"; // Mislabeled as 'Customer Report Selection' on the design...
        CSVBuffer: Record "CSV Buffer" temporary; // Buffer is not great for input, but not bad for output
        LineNo: Integer;
    begin
        // Make sure the setup exists:
        IF NOT VersaSetup.GET THEN ERROR('Could not get VersaPay Setup');

        // Lets Process based on the type of File:
        CASE FileType of
            'CUSTOMER':
                begin
                    // Make sure an Out Folder and Delimeter are set:
                    IF NOT ((STRLEN(VersaSetup."Out Folder") > 0) OR NOT (STRLEN(VersaSetup."File Delimeter") > 0)) THEN ERROR('Output Files require an Out Folder and File Delimeter to generate output files.');

                    // Generate a Filename:
                    OutFilename := VersaSetup."Out Folder" + '/customer' + FORMAT(CURRENTDATETIME(), 0, '<Year4><Month,2><Day,2><Hours24><Minutes,2><Seconds,2>') + '.csv';

                    // Prepare the CSV Buffer:
                    CSVBuffer.DeleteAll;
                    LineNo := 1;

                    // Add a Header Line:
                    CSVBuffer.InsertEntry(LineNo, 1, 'identifier');
                    CSVBuffer.InsertEntry(LineNo, 2, 'parent_identifier');
                    CSVBuffer.InsertEntry(LineNo, 3, 'name');
                    CSVBuffer.InsertEntry(LineNo, 4, 'credit_limit_cents');
                    CSVBuffer.InsertEntry(LineNo, 5, 'terms_value');
                    CSVBuffer.InsertEntry(LineNo, 6, 'terms_type');
                    CSVBuffer.InsertEntry(LineNo, 7, 'contact_email');
                    CSVBuffer.InsertEntry(LineNo, 8, 'CC_Contact_Email');
                    CSVBuffer.InsertEntry(LineNo, 9, 'telephone');
                    CSVBuffer.InsertEntry(LineNo, 10, 'Fax');
                    CSVBuffer.InsertEntry(LineNo, 11, 'address_1');
                    CSVBuffer.InsertEntry(LineNo, 12, 'address_2');
                    CSVBuffer.InsertEntry(LineNo, 13, 'city');
                    CSVBuffer.InsertEntry(LineNo, 14, 'postal_code');
                    CSVBuffer.InsertEntry(LineNo, 15, 'province');
                    CSVBuffer.InsertEntry(LineNo, 16, 'country');
                    CSVBuffer.InsertEntry(LineNo, 17, 'tags');
                    // Increment Line
                    LineNo := LineNo + 1;

                    // Write the File:
                    //CSVBuffer.SaveData(OutFileName, VersaSetup."File Delimeter");

                    // This will roll the Customer list, and for each customer grab and export the Email lines.
                    Customer.Reset;
                    Customer.SETFILTER("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    Customer.SETFILTER("No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    Customer.SETFILTER("Customer Posting Group", '<>JAPAN');
                    IF Customer.FindSet THEN
                        REPEAT
                            // Let's Prepare our Export, Adding Fields to this line:
                            CSVBuffer.InsertEntry(LineNo, 1, strprep(Customer."No.")); // 'identifier'
                            CSVBuffer.InsertEntry(LineNo, 2, strprep(Customer."Bill-to Customer No.")); // 'parent_identifier'
                            CSVBuffer.InsertEntry(LineNo, 3, strprep(Customer.Name)); // 'name'
                            CSVBuffer.InsertEntry(LineNo, 4, strprep(Customer."Credit Limit (LCY)")); // 'credit_limit_cents'

                            // Payment terms SHOULD be a sub-table, but instead they pulled the 'Code' value and strip the leading N if it exists.  Seriously I couldn't make this up.
                            PaymentTermText := Customer."Payment Terms Code";
                            IF COPYSTR(PaymentTermText, 1, 1) = 'N' THEN PaymentTermText := COPYSTR(PaymentTermText, 2);
                            CSVBuffer.InsertEntry(LineNo, 5, strprep(PaymentTermText)); // 'terms_value' - but with the N stripped off...
                            CSVBuffer.InsertEntry(LineNo, 6, 'Day'); // 'terms_type'

                            // To get the Email Lines, we need to look for TWO kinds of Email.  First, we look for the Invoice and use it if found:
                            // Note: We COULD switch to the Bill-to Customer for this, but since the Jet isn't doing that, I'm not going to yet.
                            // 7: First, use the first Invoicing or Statement one if found:
                            CustomerEmail := '';
                            CustomRepSel.Reset();
                            CustomRepSel.SETRANGE("Source Type", 18);
                            CustomRepSel.SETRANGE("Source No.", Customer."No.");
                            CustomRepSel.SETRANGE("Usage", CustomRepSel."Usage"::"S.Invoice"); // Select for Invoice
                            IF CustomRepSel.FINDSET THEN BEGIN // Customer Type (Table 18)
                                CSVBuffer.InsertEntry(LineNo, 7, strprep(CustomRepSel."Send To Email")); // 'contact_email'
                                CustomerEmail := CustomRepSel."Send To Email";
                            END ELSE BEGIN
                                // IF it's NOT found, we attempt to use the STATEMENT email:
                                CustomRepSel.Reset();
                                CustomRepSel.SETRANGE("Source Type", 18);
                                CustomRepSel.SETRANGE("Source No.", Customer."No.");
                                CustomRepSel.SETRANGE("Usage", CustomRepSel."Usage"::"C.Statement"); // Select for Statement
                                IF CustomRepSel.FINDSET THEN BEGIN // Customer Type (Table 18)
                                    // Use Statement if found:
                                    CSVBuffer.InsertEntry(LineNo, 7, strprep(CustomRepSel."Send To Email")); // 'contact_email'
                                    CustomerEmail := CustomRepSel."Send To Email";
                                END ELSE BEGIN
                                    // Nothing Found.  Add Empty Field:
                                    CSVBuffer.InsertEntry(LineNo, 7, ''); // 'contact_email'     
                                END;
                            END;
                            // 8: Use the NEXT Invoice or Statement that doesn't match the first one, if we have a first one.
                            IF STRLEN(CustomerEmail) > 0 THEN BEGIN
                                CustomRepSel.Reset();
                                CustomRepSel.SETRANGE("Source Type", 18);
                                CustomRepSel.SETRANGE("Source No.", Customer."No.");
                                CustomRepSel.SETRANGE("Usage", CustomRepSel."Usage"::"S.Invoice"); // Select for Invoice
                                CustomRepSel.SETFILTER("Send To Email", '<>' + CustomerEmail);
                                IF CustomRepSel.FINDSET THEN BEGIN // Customer Type (Table 18)
                                    CSVBuffer.InsertEntry(LineNo, 8, strprep(CustomRepSel."Send To Email")); // 'contact_email'
                                END ELSE BEGIN
                                    // IF it's NOT found, we attempt to use the STATEMENT email:
                                    CustomRepSel.Reset();
                                    CustomRepSel.SETRANGE("Source Type", 18);
                                    CustomRepSel.SETRANGE("Source No.", Customer."No.");
                                    CustomRepSel.SetRange("Usage", CustomRepSel."Usage"::"C.Statement"); // Select for Statement
                                    CustomRepSel.SETFILTER("Send To Email", '<>' + CustomerEmail);
                                    IF CustomRepSel.FINDSET THEN BEGIN // Customer Type (Table 18)
                                        // Use Statement if found:
                                        CSVBuffer.InsertEntry(LineNo, 8, strprep(CustomRepSel."Send To Email")); // 'contact_email'
                                    END ELSE BEGIN
                                        // Nothing Found.  Add Empty Field:
                                        CSVBuffer.InsertEntry(LineNo, 8, ''); // 'contact_email'     
                                    END;
                                END;
                            END ELSE
                                CSVBuffer.InsertEntry(LineNo, 8, ''); // 'contact_email' 

                            CSVBuffer.InsertEntry(LineNo, 9, strprep(Customer."Phone No.")); // 'telephone'
                            CSVBuffer.InsertEntry(LineNo, 10, strprep(Customer."Fax No.")); // 'Fax'
                            CSVBuffer.InsertEntry(LineNo, 11, strprep(Customer.Address)); // 'address_1'
                            CSVBuffer.InsertEntry(LineNo, 12, strprep(Customer."Address 2")); // 'address_2'
                            CSVBuffer.InsertEntry(LineNo, 13, strprep(Customer.City)); // 'city'
                            CSVBuffer.InsertEntry(LineNo, 14, strprep(Customer."Post Code")); // 'postal_code'
                            CSVBuffer.InsertEntry(LineNo, 15, strprep(Customer.County)); // (State) 'province'
                            CSVBuffer.InsertEntry(LineNo, 16, strprep(Customer."Country/Region Code")); // 'country'
                            CSVBuffer.InsertEntry(LineNo, 17, strprep(CompanyName + ';' + Customer."Global Dimension 1 Code" + ';' + Customer."Gen. Bus. Posting Group")); // 'tags'

                            // Increment Line
                            LineNo := LineNo + 1;
                        UNTIL Customer.Next = 0;

                    // Write the File:
                    CSVBuffer.SaveData(OutFileName, VersaSetup."File Delimeter");
                end;
            'INVOICE':
                begin
                    // Make sure an Out Folder and Delimeter are set:
                    IF NOT ((STRLEN(VersaSetup."Out Folder") > 0) OR NOT (STRLEN(VersaSetup."File Delimeter") > 0)) THEN ERROR('Output Files require an Out Folder and File Delimeter to generate output files.');

                    // Generate a Filename:
                    OutFilename := VersaSetup."Out Folder" + '/invoice' + FORMAT(CURRENTDATETIME(), 0, '<Year4><Month,2><Day,2><Hours24><Minutes,2><Seconds,2>') + '.csv';

                    // Prepare the CSV Buffer:
                    CSVBuffer.DeleteAll;
                    LineNo := 1;

                    // Add a Header Line:
                    CSVBuffer.InsertEntry(LineNo, 1, 'number');
                    CSVBuffer.InsertEntry(LineNo, 2, 'item_code');
                    CSVBuffer.InsertEntry(LineNo, 3, 'line_item_number');
                    CSVBuffer.InsertEntry(LineNo, 4, 'line_item_description');
                    CSVBuffer.InsertEntry(LineNo, 5, 'line_item_quantity');
                    CSVBuffer.InsertEntry(LineNo, 6, 'line_item_unit_cost');
                    CSVBuffer.InsertEntry(LineNo, 7, 'line_item_amount');
                    CSVBuffer.InsertEntry(LineNo, 8, 'line_item_discount_amt');
                    CSVBuffer.InsertEntry(LineNo, 9, 'line_item_total_amount');
                    CSVBuffer.InsertEntry(LineNo, 10, 'subtotal');
                    CSVBuffer.InsertEntry(LineNo, 11, 'tax');
                    CSVBuffer.InsertEntry(LineNo, 12, 'amount');
                    CSVBuffer.InsertEntry(LineNo, 13, 'balance');
                    CSVBuffer.InsertEntry(LineNo, 14, 'shipment_date');
                    CSVBuffer.InsertEntry(LineNo, 15, 'purchase_order_number');
                    CSVBuffer.InsertEntry(LineNo, 16, 'sales_order_number');
                    CSVBuffer.InsertEntry(LineNo, 17, 'customer_identifier');
                    CSVBuffer.InsertEntry(LineNo, 18, 'division');
                    CSVBuffer.InsertEntry(LineNo, 19, 'date');
                    CSVBuffer.InsertEntry(LineNo, 20, 'order_date');
                    CSVBuffer.InsertEntry(LineNo, 21, 'due_date');
                    CSVBuffer.InsertEntry(LineNo, 22, 'shipping_name');
                    CSVBuffer.InsertEntry(LineNo, 23, 'shipping_address_1');
                    CSVBuffer.InsertEntry(LineNo, 24, 'shipping_address_2');
                    CSVBuffer.InsertEntry(LineNo, 25, 'shipping_city');
                    CSVBuffer.InsertEntry(LineNo, 26, 'shipping_postal_code');
                    CSVBuffer.InsertEntry(LineNo, 27, 'shipping_province');
                    CSVBuffer.InsertEntry(LineNo, 28, 'shipping_country');
                    CSVBuffer.InsertEntry(LineNo, 29, 'terms');
                    CSVBuffer.InsertEntry(LineNo, 30, 'rep');
                    CSVBuffer.InsertEntry(LineNo, 31, 'via');
                    CSVBuffer.InsertEntry(LineNo, 32, 'your_reference');
                    CSVBuffer.InsertEntry(LineNo, 33, 'finance_charge');
                    CSVBuffer.InsertEntry(LineNo, 34, 'nsf_fee');
                    CSVBuffer.InsertEntry(LineNo, 35, 'convenience_fee');

                    // Increment Line
                    LineNo := LineNo + 1;

                    // NOW THE INSANITY.
                    // Given that the document number is reused in the customer ledger entry, we'll likely need to do a 'total' to get balance (and not a first)
                    // AND that we're supposed to filter out be remaining - we'll use the ledger entry to lead what should be copied to avoid the issues in the file.
                    // This will also let us cycle ONCE to get the 'AR Beg Bal' lines as well - since they appear to use the same limits.
                    // Part 1: Invoices
                    SalesInvoiceHeader.Reset;
                    SalesInvoiceHeader.SETFILTER("Shortcut Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    SalesInvoiceHeader.SETFILTER("Remaining Amount", '<>0');
                    SalesInvoiceHeader.SETFILTER("Sell-To Customer No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    SalesInvoiceHeader.SETFILTER("Customer Posting Group", '<>JAPAN');
                    IF SalesInvoiceHeader.FindSet THEN
                        REPEAT
                            SalesInvoiceLine.RESET;
                            SalesInvoiceLine.SetFilter(Amount, '<>0');
                            SalesInvoiceLine.SETFILTER("Document No.", SalesInvoiceHeader."No.");
                            IF SalesInvoiceLine.FindSet THEN
                                REPEAT
                                    // We should have everything they need now:
                                    CSVBuffer.InsertEntry(LineNo, 1, strprep(SalesInvoiceLine."Document No."));  //number
                                    CSVBuffer.InsertEntry(LineNo, 2, strprep(SalesInvoiceLine."No."));  //item_code
                                    CSVBuffer.InsertEntry(LineNo, 3, strprep(SalesInvoiceLine."Line No."));  //line_item_number
                                    CSVBuffer.InsertEntry(LineNo, 4, strprep(SalesInvoiceLine."Description"));  //line_item_description
                                    CSVBuffer.InsertEntry(LineNo, 5, strprep(SalesInvoiceLine."Quantity"));  //line_item_quantity
                                    CSVBuffer.InsertEntry(LineNo, 6, strprep(SalesInvoiceLine."Unit Price"));  //line_item_unit_cost
                                    CSVBuffer.InsertEntry(LineNo, 7, strprep(SalesInvoiceLine."Unit Price" * SalesInvoiceLIne.Quantity));  //line_item_amount
                                    CSVBuffer.InsertEntry(LineNo, 8, strprep(SalesInvoiceLine."Line Discount Amount"));  //line_item_discount_amt
                                    CSVBuffer.InsertEntry(LineNo, 9, strprep(SalesInvoiceLine."Amount"));  //line_item_total_amount
                                    SalesInvoiceHeader.CalcFields("Amount", "Amount Including VAT", "Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                                    CSVBuffer.InsertEntry(LineNo, 10, strprep(SalesInvoiceHeader."Amount"));  //subtotal
                                    CSVBuffer.InsertEntry(LineNo, 11, strprep(SalesInvoiceHeader."Amount Including VAT" - SalesInvoiceHeader."Amount"));  //tax (Difference between these two?)
                                    CSVBuffer.InsertEntry(LineNo, 12, strprep(SalesInvoiceHeader."Amount Including VAT"));  //amount
                                    CSVBuffer.InsertEntry(LineNo, 13, strprep(SalesInvoiceHeader."Remaining Amount"));  //balance
                                    CSVBuffer.InsertEntry(LineNo, 14, strprep(SalesInvoiceLine."Shipment Date"));  //shipment_date
                                    CSVBuffer.InsertEntry(LineNo, 15, strprep(SalesInvoiceHeader."External Document No."));  //purchase_order_number
                                    CSVBuffer.InsertEntry(LineNo, 16, strprep(SalesInvoiceLine."Order No."));  //sales_order_number
                                    CSVBuffer.InsertEntry(LineNo, 17, strprep(SalesInvoiceHeader."Bill-To Customer No."));  //customer_identifier
                                    CSVBuffer.InsertEntry(LineNo, 18, strprep(CompanyName));  //division (Setting to Company)
                                    CSVBuffer.InsertEntry(LineNo, 19, strprep(SalesInvoiceLine."Posting Date"));  //date
                                    CSVBuffer.InsertEntry(LineNo, 20, strprep(SalesInvoiceHeader."Order Date"));  //order_date
                                    CSVBuffer.InsertEntry(LineNo, 21, strprep(SalesInvoiceHeader."Due Date"));  //due_date
                                    CSVBuffer.InsertEntry(LineNo, 22, strprep(SalesInvoiceHeader."Ship-to Name"));  //shipping_name
                                    CSVBuffer.InsertEntry(LineNo, 23, strprep(SalesInvoiceHeader."Ship-to Address"));  //shipping_address_1
                                    CSVBuffer.InsertEntry(LineNo, 24, strprep(SalesInvoiceHeader."Ship-to Address 2"));  //shipping_address_2
                                    CSVBuffer.InsertEntry(LineNo, 25, strprep(SalesInvoiceHeader."Ship-to City"));  //shipping_city
                                    CSVBuffer.InsertEntry(LineNo, 26, strprep(SalesInvoiceHeader."Ship-to Post Code"));  //shipping_postal_code
                                    CSVBuffer.InsertEntry(LineNo, 27, strprep(SalesInvoiceHeader."Ship-to County"));  //shipping_province
                                    CSVBuffer.InsertEntry(LineNo, 28, strprep(SalesInvoiceHeader."Ship-to Country/Region Code"));  //shipping_country
                                    CSVBuffer.InsertEntry(LineNo, 29, strprep(SalesInvoiceHeader."Payment Terms Code"));  //terms
                                    CSVBuffer.InsertEntry(LineNo, 30, strprep(SalesInvoiceHeader."Salesperson Code"));  //rep
                                    CSVBuffer.InsertEntry(LineNo, 31, strprep(SalesInvoiceHeader."Shipping Agent Code"));  //via
                                    CSVBuffer.InsertEntry(LineNo, 32, strprep(SalesInvoiceHeader."Your Reference"));  //your_reference
                                    CSVBuffer.InsertEntry(LineNo, 33, '0.01');  //finance_charge
                                    CSVBuffer.InsertEntry(LineNo, 34, '45');  //nsf_fee
                                    CSVBuffer.InsertEntry(LineNo, 35, '2.5');  //convenience_fee

                                    // Increment Line
                                    LineNo := LineNo + 1;

                                UNTIL SalesInvoiceLine.Next = 0;
                        UNTIL SalesInvoiceHeader.Next = 0;
                    // Part 2: Credits
                    CreditHeader.Reset;
                    CreditHeader.SETFILTER("Shortcut Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    CreditHeader.SETFILTER("Remaining Amount", '<>0');
                    CreditHeader.SETFILTER("Sell-To Customer No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    CreditHeader.SETFILTER("Customer Posting Group", '<>JAPAN');
                    IF CreditHeader.FindSet THEN
                        REPEAT
                            CreditLine.RESET;
                            CreditLine.setfilter(Amount, '<>0');
                            CreditLine.SETFILTER("Document No.", CreditHeader."No.");
                            IF CreditLine.FindSet THEN
                                REPEAT
                                    // We should have everything they need now:
                                    CSVBuffer.InsertEntry(LineNo, 1, strprep(CreditLine."Document No."));  //number
                                    CSVBuffer.InsertEntry(LineNo, 2, strprep(CreditLine."No."));  //item_code
                                    CSVBuffer.InsertEntry(LineNo, 3, strprep(CreditLine."Line No."));  //line_item_number
                                    CSVBuffer.InsertEntry(LineNo, 4, strprep(CreditLine."Description"));  //line_item_description
                                    CSVBuffer.InsertEntry(LineNo, 5, strprep(CreditLine."Quantity"));  //line_item_quantity
                                    CSVBuffer.InsertEntry(LineNo, 6, strprep(-1 * CreditLine."Unit Price"));  //line_item_unit_cost
                                    CSVBuffer.InsertEntry(LineNo, 7, strprep(-1 * (CreditLine."Unit Price" * CreditLine."Quantity")));  //line_item_amount
                                    CSVBuffer.InsertEntry(LineNo, 8, strprep(CreditLine."Line Discount Amount"));  //line_item_discount_amt
                                    CSVBuffer.InsertEntry(LineNo, 9, strprep(-1 * CreditLine."Amount"));  //line_item_total_amount
                                    CreditHeader.CalcFields("Amount", "Amount Including VAT", "Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                                    CSVBuffer.InsertEntry(LineNo, 10, strprep(-1 * CreditHeader."Amount"));  //subtotal
                                    CSVBuffer.InsertEntry(LineNo, 11, strprep(-1 * (CreditHeader."Amount Including VAT" - CreditHeader."Amount")));  //tax (Difference between these two?)
                                    CSVBuffer.InsertEntry(LineNo, 12, strprep(-1 * CreditHeader."Amount Including VAT"));  //amount
                                    CSVBuffer.InsertEntry(LineNo, 13, strprep(CreditHeader."Remaining Amount"));  //balance
                                    CSVBuffer.InsertEntry(LineNo, 14, strprep(CreditLine."Shipment Date"));  //shipment_date
                                    CSVBuffer.InsertEntry(LineNo, 15, strprep(CreditHeader."External Document No."));  //purchase_order_number
                                    CSVBuffer.InsertEntry(LineNo, 16, '');  //sales_order_number (Blank)
                                    CSVBuffer.InsertEntry(LineNo, 17, strprep(CreditHeader."Bill-To Customer No."));  //customer_identifier
                                    CSVBuffer.InsertEntry(LineNo, 18, strprep(CompanyName));  //division (Setting to Company)
                                    CSVBuffer.InsertEntry(LineNo, 19, strprep(CreditLine."Posting Date"));  //date
                                    CSVBuffer.InsertEntry(LineNo, 20, '');  //order_date (Blank)
                                    CSVBuffer.InsertEntry(LineNo, 21, strprep(CreditHeader."Due Date"));  //due_date
                                    CSVBuffer.InsertEntry(LineNo, 22, strprep(CreditHeader."Ship-to Name"));  //shipping_name
                                    CSVBuffer.InsertEntry(LineNo, 23, strprep(CreditHeader."Ship-to Address"));  //shipping_address_1
                                    CSVBuffer.InsertEntry(LineNo, 24, strprep(CreditHeader."Ship-to Address 2"));  //shipping_address_2
                                    CSVBuffer.InsertEntry(LineNo, 25, strprep(CreditHeader."Ship-to City"));  //shipping_city
                                    CSVBuffer.InsertEntry(LineNo, 26, strprep(CreditHeader."Ship-to Post Code"));  //shipping_postal_code
                                    CSVBuffer.InsertEntry(LineNo, 27, strprep(CreditHeader."Ship-to County"));  //shipping_province
                                    CSVBuffer.InsertEntry(LineNo, 28, strprep(CreditHeader."Ship-to Country/Region Code"));  //shipping_country
                                    CSVBuffer.InsertEntry(LineNo, 29, strprep(CreditHeader."Payment Terms Code"));  //terms
                                    CSVBuffer.InsertEntry(LineNo, 30, strprep(CreditHeader."Salesperson Code"));  //rep
                                    CSVBuffer.InsertEntry(LineNo, 31, '');  //via (Blank)
                                    CSVBuffer.InsertEntry(LineNo, 32, strprep(CreditHeader."Your Reference"));  //your_reference
                                    CSVBuffer.InsertEntry(LineNo, 33, '');  //finance_charge (Blank Credit only?)
                                    CSVBuffer.InsertEntry(LineNo, 34, '');  //nsf_fee (Blank Credit only?)
                                    CSVBuffer.InsertEntry(LineNo, 35, '');  //convenience_fee (Blank Credit only?)

                                    // Increment Line
                                    LineNo := LineNo + 1;

                                UNTIL CreditLine.Next = 0;
                        UNTIL CreditHeader.Next = 0;

                    // Part 3: ... Balances?  Ledger entries?  I'm unsure.
                    CustLedger.Reset;
                    CustLedger.SETFILTER("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    CustLedger.SETFILTER("Remaining Amount", '<>0');
                    //CustLedger.SETFILTER("Document Type", 'Credit Memo|Reminder|Finance Charge Memo|Invoice'); // Flipped because I didn't see any payments in he other one, so assuming this to match file
                    CustLedger.SETFILTER("Description", 'AR BEG BALANCE');
                    CustLedger.SETFILTER("Customer No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    CustLedger.SETFILTER("Customer Posting Group", '<>JAPAN');
                    IF CustLedger.FindSet THEN
                        REPEAT
                            CSVBuffer.InsertEntry(LineNo, 1, strprep(CustLedger."Document No."));  // 'number'
                            CSVBuffer.InsertEntry(LineNo, 2, strprep(CustLedger."Document Type"));  // 'item_code'
                            CSVBuffer.InsertEntry(LineNo, 3, '');  // 'line_item_number'
                            CSVBuffer.InsertEntry(LineNo, 4, strprep(CustLedger."Description"));  // 'line_item_description'
                            CSVBuffer.InsertEntry(LineNo, 5, '1');  // 'line_item_quantity'
                            CSVBuffer.InsertEntry(LineNo, 6, '0');  // 'line_item_unit_cost'
                            CustLedger.CalcFields("Amount", "Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                            CSVBuffer.InsertEntry(LineNo, 7, strprep(CustLedger."Amount"));  // 'line_item_amount'
                            CSVBuffer.InsertEntry(LineNo, 8, '0');  // 'line_item_discount_amt'
                            CSVBuffer.InsertEntry(LineNo, 9, strprep(CustLedger."Amount"));  // 'line_item_total_amount'
                            CSVBuffer.InsertEntry(LineNo, 10, strprep(CustLedger."Amount"));  // 'subtotal'
                            CSVBuffer.InsertEntry(LineNo, 11, '0');  // 'tax'
                            CSVBuffer.InsertEntry(LineNo, 12, strprep(CustLedger."Amount"));  // 'amount'
                            CSVBuffer.InsertEntry(LineNo, 13, strprep(CustLedger."Remaining Amount"));  // 'balance'
                            CSVBuffer.InsertEntry(LineNo, 14, '');  // 'shipment_date'
                            CSVBuffer.InsertEntry(LineNo, 15, '');  // 'purchase_order_number'
                            CSVBuffer.InsertEntry(LineNo, 16, '');  // 'sales_order_number'
                            CSVBuffer.InsertEntry(LineNo, 17, strprep(CustLedger."Sell-To Customer No."));  // 'customer_identifier'
                            CSVBuffer.InsertEntry(LineNo, 18, strprep(CompanyName));  // 'division'
                            CSVBuffer.InsertEntry(LineNo, 19, strprep(CustLedger."Posting Date"));  // 'date'
                            CSVBuffer.InsertEntry(LineNo, 20, '');  // 'order_date'
                            CSVBuffer.InsertEntry(LineNo, 21, strprep(CustLedger."Due Date"));  // 'due_date'
                            // Now unfortunately we have no choice but to load Customer to get this:
                            IF Customer.Get(CustLedger."Sell-To Customer No.") THEN BEGIN
                                CSVBuffer.InsertEntry(LineNo, 22, strprep(Customer."Name"));  // 'shipping_name'
                                CSVBuffer.InsertEntry(LineNo, 23, strprep(Customer."Address"));  // 'shipping_address_1'
                                CSVBuffer.InsertEntry(LineNo, 24, strprep(Customer."Address 2"));  // 'shipping_address_2'
                                CSVBuffer.InsertEntry(LineNo, 25, strprep(Customer."City"));  // 'shipping_city'
                                CSVBuffer.InsertEntry(LineNo, 26, strprep(Customer."Post Code"));  // 'shipping_postal_code'
                                CSVBuffer.InsertEntry(LineNo, 27, strprep(Customer."County"));  // 'shipping_province'
                                CSVBuffer.InsertEntry(LineNo, 28, strprep(Customer."Country/Region Code"));  // 'shipping_country'
                                CSVBuffer.InsertEntry(LineNo, 29, strprep(Customer."Payment Terms Code"));  // 'terms'
                                CSVBuffer.InsertEntry(LineNo, 30, strprep(Customer."Salesperson Code"));  // 'rep'
                                CSVBuffer.InsertEntry(LineNo, 31, strprep(Customer."Shipping Agent Code"));  // 'via'
                                CSVBuffer.InsertEntry(LineNo, 32, '');  // 'your_reference' This is not set in any of the records, so ignoring
                                CSVBuffer.InsertEntry(LineNo, 33, '0.01');  //finance_charge
                                CSVBuffer.InsertEntry(LineNo, 34, '45');  //nsf_fee
                                CSVBuffer.InsertEntry(LineNo, 35, '2.5');  //convenience_fee
                            END ELSE begin
                                // Customer not found, sending blanks:
                                CSVBuffer.InsertEntry(LineNo, 22, '');
                                CSVBuffer.InsertEntry(LineNo, 23, '');
                                CSVBuffer.InsertEntry(LineNo, 24, '');
                                CSVBuffer.InsertEntry(LineNo, 25, '');
                                CSVBuffer.InsertEntry(LineNo, 26, '');
                                CSVBuffer.InsertEntry(LineNo, 27, '');
                                CSVBuffer.InsertEntry(LineNo, 28, '');
                                CSVBuffer.InsertEntry(LineNo, 29, '');
                                CSVBuffer.InsertEntry(LineNo, 30, '');
                                CSVBuffer.InsertEntry(LineNo, 31, '');
                                CSVBuffer.InsertEntry(LineNo, 32, '');
                                CSVBuffer.InsertEntry(LineNo, 33, '0.01');  //finance_charge
                                CSVBuffer.InsertEntry(LineNo, 34, '45');  //nsf_fee
                                CSVBuffer.InsertEntry(LineNo, 35, '2.5');  //convenience_fee
                            end;

                            // Increment Line
                            LineNo := LineNo + 1;
                        UNTIL CustLedger.Next = 0;


                    // Write the File:
                    CSVBuffer.SaveData(OutFileName, VersaSetup."File Delimeter");
                end;
            /*'PAYMENT_OUT'://>>RPS Commenting
                begin
                    // Make sure an Out Folder and Delimeter are set:
                    IF NOT ((STRLEN(VersaSetup."Out Folder") > 0) OR NOT (STRLEN(VersaSetup."File Delimeter") > 0)) THEN ERROR('Output Files require an Out Folder and File Delimeter to generate output files.');

                    // Generate a Filename:
                    OutFilename := VersaSetup."Out Folder" + '/paymentbc' + FORMAT(CURRENTDATETIME(), 0, '<Year4><Month,2><Day,2><Hours24><Minutes,2><Seconds,2>') + '.csv';

                    // Prepare the CSV Buffer:
                    CSVBuffer.DeleteAll;
                    LineNo := 1;

                    // Add a header line:
                    CSVBuffer.InsertEntry(LineNo, 1, 'identifier');
                    CSVBuffer.InsertEntry(LineNo, 2, 'date');
                    CSVBuffer.InsertEntry(LineNo, 3, 'currency');
                    CSVBuffer.InsertEntry(LineNo, 4, 'customer_identifier');
                    CSVBuffer.InsertEntry(LineNo, 5, 'customer_name');
                    CSVBuffer.InsertEntry(LineNo, 6, 'payment_note');
                    CSVBuffer.InsertEntry(LineNo, 7, 'payment_total');

                    // Increment Line
                    LineNo := LineNo + 1;

                    CustLedger.Reset;
                    CustLedger.SETFILTER("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    CustLedger.SETFILTER("Remaining Amount", '<>0');
                    // I guess a payment here is anything NOT sent above?... twice?
                    CustLedger.SETFILTER("Document Type", '<>Credit Memo&<>Reminder&<>Finance Charge Memo&<>Invoice');
                    CustLedger.SETFILTER("Customer No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    CustLedger.SETFILTER("Customer Posting Group", '<>JAPAN');
                    IF CustLedger.FindSet THEN
                        REPEAT
                            CSVBuffer.InsertEntry(LineNo, 1, strprep(CustLedger."Document No." + ' - ' + FORMAT(CustLedger."Entry No."))); // identifier
                            CSVBuffer.InsertEntry(LineNo, 2, strprep(CustLedger."Posting Date")); // date
                            CSVBuffer.InsertEntry(LineNo, 3, 'USD'); // currency
                            CSVBuffer.InsertEntry(LineNo, 4, strprep(CustLedger."Customer No.")); // customer_identifier
                            Customer.Reset;
                            IF Customer.GET(CustLedger."Customer No.") THEN
                                CSVBuffer.InsertEntry(LineNo, 5, strprep(Customer."Name")) // customer_name
                            ELSE
                                CSVBuffer.InsertEntry(LineNo, 5, strprep(CustLedger."Customer Name")); // customer_name (If not found)
                            //CSVBuffer.InsertEntry(LineNo, 6, strprep(FORMAT(CustLedger."Document Type") + ' - ' + CustLedger."Description")); // payment_note
                            CSVBuffer.InsertEntry(LineNo, 6, strprep(CompanyName + ' DocType:' + FORMAT(CustLedger."Document Type") + ' Description:' + CustLedger.Description)); // payment_note
                            CustLedger.CalcFields("Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                            CSVBuffer.InsertEntry(LineNo, 7, strprep(-1 * CustLedger."Remaining Amount")); // payment_total

                            // Increment Line
                            LineNo := LineNo + 1;
                        UNTIL CustLedger.Next = 0;

                    // Write the File:
                    CSVBuffer.SaveData(OutFileName, VersaSetup."File Delimeter");
                end;
                *///<<RPS Commenting
            'RECON':
                begin
                    // Make sure an Out Folder and Delimeter are set:
                    IF NOT ((STRLEN(VersaSetup."Out Folder") > 0) OR NOT (STRLEN(VersaSetup."File Delimeter") > 0)) THEN ERROR('Output Files require an Out Folder and File Delimeter to generate output files.');

                    // Generate a Filename:
                    OutFilename := VersaSetup."Out Folder" + '/recon' + FORMAT(CURRENTDATETIME(), 0, '<Year4><Month,2><Day,2><Hours24><Minutes,2><Seconds,2>') + '.csv';

                    // Prepare the CSV Buffer:
                    CSVBuffer.DeleteAll;
                    LineNo := 1;

                    // Add a header line:
                    CSVBuffer.InsertEntry(LineNo, 1, 'reconciliation_invoice_number');
                    CSVBuffer.InsertEntry(LineNo, 2, 'balance');
                    CSVBuffer.InsertEntry(LineNo, 3, 'division');

                    // Increment Line
                    LineNo := LineNo + 1;

                    CustLedger.Reset;
                    CustLedger.SETFILTER("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    CustLedger.SETFILTER("Remaining Amount", '<>0');
                    // I guess a payment here is anything NOT sent above?... twice?
                    // Removing this filter temporarily
                    CustLedger.SETFILTER("Document Type", 'Credit Memo|Reminder|Finance Charge Memo|Invoice');
                    CustLedger.SETFILTER("Customer No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    CustLedger.SETFILTER("Customer Posting Group", '<>JAPAN');
                    IF CustLedger.FindSet THEN
                        REPEAT
                            CSVBuffer.InsertEntry(LineNo, 1, strprep(CustLedger."Document No.")); // 'reconciliation_invoice_number'
                            CustLedger.CalcFields("Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                            CSVBuffer.InsertEntry(LineNo, 2, strprep(CustLedger."Remaining Amount")); // 'balance'
                            CSVBuffer.InsertEntry(LineNo, 3, strprep(CompanyName)); // 'division'

                            // Increment Line
                            LineNo := LineNo + 1;
                        UNTIL CustLedger.Next = 0;

                    // Write the File:
                    CSVBuffer.SaveData(OutFileName, VersaSetup."File Delimeter");
                end;
        END;
    end;

    procedure strprep(In_Value: Variant) Out_Text: Text
    Begin
        // This function is to prepare the output for the Export.  That means removing commas from numeric values,
        // Making sure the date is in the right format for transmission
        // and enclosing strings in quotes (which is implied by the examples, but not in the descrition)
        CASE true of
            In_Value.IsCode():
                begin
                    // Lets Escape any " and surround it with ""
                    Out_Text := In_Value;
                    Out_Text := Out_Text.Replace('"', '""');
                    Out_Text := '"' + Out_Text + '"';
                end;
            In_Value.IsText():
                begin
                    // Lets Escape any " and surround it with ""
                    Out_Text := In_Value;
                    Out_Text := Out_Text.Replace('"', '""');
                    Out_Text := '"' + Out_Text + '"';
                end;
            In_Value.IsDecimal():
                begin
                    // By default, Format will add commas... which we don't need.
                    Out_Text := FORMAT(In_Value, 0, 1); // 1 = <Sign><Integer><Decimals>
                end;
            ELSE BEGIN
                // Just spit out normal Formatting to force to Text:
                Out_Text := FORMAT(In_Value);
            END;
        END;
    End;

    procedure CodedLineField(In_Header: Dictionary of [Integer, Text]; In_Field: Text) Out_Val: Integer
    var
        X: Integer;
        Buffer: Text;
    begin
        // This function simply looks for a field, and returns the number - for use in pulling values
        FOR X := 1 TO In_Header.Count DO begin
            Buffer := In_Header.Get(X);
            if UPPERCASE(Buffer) = UPPERCASE(In_Field) THEN begin
                Out_Val := X;
                EXIT;
            end;
        end;

        // If you got this far, you didn't find it - that's an error-able offense:
        Error('Error in Import - Could not find column ' + In_Field + ' In Data');
    end;

    procedure CodedLineValue(In_Array: Dictionary of [Integer, Text]; In_Field: Integer; var Out_Value: Variant)
    var
        txtNewDate: Text;
        txtNewTime: Text;
        OutDate: Date;
        OutTime: Time;
        OutDateTime: DateTime;
    begin
        // This function takes the Field Number and returns the Value
        // However it also will try and detect the TYPE and convert it properly to return the right return datatype
        // For now, just return as is - we can work on Value processing later:
        IF In_Array.ContainsKey(In_Field) then BEGIN
            // Grab Value, then we'll adjust it to another type if we detect one:
            Out_Value := In_Array.Get(In_Field); // Just return it as is if we don't have another way.

            // Type Detection:  If we find Dashes, T's, Etc - then we know we have to convert this to another Datatype.
            // Date Detection (Length>11)
            CASE true of
                (STRLEN(FORMAT(Out_Value)) = 10) OR (STRLEN(FORMAT(Out_Value)) = 25):
                    BEGIN
                        IF (STRLEN(FORMAT(Out_Value)) = 10) THEN
                            IF (COPYSTR(FORMAT(Out_Value), 5, 1) = '-') AND (COPYSTR(FORMAT(Out_Value), 8, 1) = '-') THEN begin
                                // Found Date in format yyyy-MM-dd
                                txtNewDate := COPYSTR(FORMAT(Out_Value), 6, 2)
                                    + '/' + COPYSTR(FORMAT(Out_Value), 9, 2)
                                    + '/' + COPYSTR(FORMAT(Out_Value), 1, 4);
                                EVALUATE(OutDate, txtNewDate);
                                Out_Value := OutDate;
                                EXIT;
                            end;
                        IF (STRLEN(FORMAT(Out_Value)) = 25) THEN
                            IF (COPYSTR(FORMAT(Out_Value), 5, 1) = '-') AND (COPYSTR(FORMAT(Out_Value), 8, 1) = '-') AND (COPYSTR(FORMAT(Out_Value), 11, 1) = 'T') THEN begin
                                // Found Date in format yyyy-MM-ddTHH:ii:ss-TT:TT for timezone offset/time
                                // Found Date in format yyyy-MM-dd
                                txtNewDate := COPYSTR(FORMAT(Out_Value), 6, 2)
                                    + '/' + COPYSTR(FORMAT(Out_Value), 9, 2)
                                    + '/' + COPYSTR(FORMAT(Out_Value), 1, 4);
                                txtNewTime := COPYSTR(FORMAT(Out_Value), 12, 2)
                                    + ':' + COPYSTR(FORMAT(Out_Value), 15, 2)
                                    + ':' + COPYSTR(FORMAT(Out_Value), 18, 2);
                                // Because nobody asked for it, I haven't rendered Timezone processing.  
                                // Would be added here if needed.
                                EVALUATE(OutDate, txtNewDate);
                                EVALUATE(OutTime, txtNewTime);
                                OutDateTime := CREATEDATETIME(OutDate, OutTime);
                                Out_Value := OutDateTime;
                                EXIT;
                            end;
                    END;
            END;
        END
        ELSE
            Error('Error in Import - Could not find column ' + FORMAT(In_Field) + ' In Data');
    end;

    procedure CodedLineToArray(In_Text: Text; In_Delimeter: Text; In_Quote: Text; var Out_Array: Dictionary of [Integer, Text])
    var
        Pos: Integer; // Position of the Current Delimeter
        Pos_Next: Integer; // Position of the Next Delimeter
        Pos_Quote: Integer; // Position of the nearest escaped Quote
        SubText: Text; // Remaining Line String
        Out_Text: Text; // Buffer for the Text Value
        CheckChar: Text; // Single Check for Quote Character
        ColNo: Integer; // Current Column Number
        Debug_Text: Text;
    begin
        // So we will roll through the Line based on passed settings and gather into the array.
        Pos := 1;
        ColNo := 1;
        WHILE (Pos > 0) and (ColNo < 100) DO BEGIN
            // Snag the Current 'Next Section' and look for a Comma using Current Position.
            SubText := COPYSTR(In_Text, Pos);

            // First, check the first char for a quote character - if you find it, then we must deal with quotes. 
            // If not, we just get the next Delimeter
            CheckChar := COPYSTR(SubText, 1, 1);
            IF (CheckChar = In_Quote) AND (STRLEN(In_Quote) > 0) THEN begin
                // Then this is a quoted String.  
                // We need to process carefully and avoid escaped values (which is "" or \" by default)
                // Unfortunately, without regex we have to walk it by hand.  So we'll loop here
                // And ignore escaped quotes until we hit another quote
                Pos_Next := Pos + 1;
                // Reset SubText to Jump to where we are scanning
                WHILE (Pos_Next < STRLEN(In_Text)) AND (Pos_Next > 0) DO begin
                    //Debug_Text := Debug_Text + FORMAT(Pos_Next) + ':';
                    SubText := COPYSTR(In_Text, Pos_Next);
                    CheckChar := COPYSTR(In_Text, Pos_Next, 1);
                    // First, Check if this is an escaped Quote:
                    IF (COPYSTR(In_Text, Pos_Next, 1) = In_Quote) AND (COPYSTR(In_Text, Pos_Next + 1, 1) = In_Quote) THEN begin
                        // Then this is an Escaped ("") character.  Ignore and move on
                        //Debug_Text := Debug_Text + 'Found Escape Quote at ' + FORMAT(Pos_Next) + ';';
                        Pos_Next := Pos_Next + 2;
                    END ELSE BEGIN
                        // Lets check for the End quote or the end of the file:
                        IF (CheckChar = In_Quote) AND ((Pos_Next + 1) > STRLEN(In_Text)) THEN begin
                            // Then we have an end quote and the end of the Line
                            //Debug_Text := Debug_Text + 'Found End of Line at ' + FORMAT(Pos_Next) + ';';
                            Pos_Next := Pos_Next + 1; // Move to the Delimeter (Or EOL) and Exit
                            BREAK;
                        END;
                        IF (CheckChar = In_Quote) AND (COPYSTR(In_Text, Pos_Next + 1, 1) <> In_Quote) THEN begin
                            // Then we have an ending quote that isn't Escaped.  We'll handle it because we're nice.
                            //Debug_Text := Debug_Text + 'Found End Quote at ' + FORMAT(Pos_Next) + ';';
                            Pos_Next := Pos_Next + 1; // Move to the Delimeter and Exit
                            BREAK;
                        END;

                        // Basically, at this point we know this is a continuing string.
                        // If we are here and we don't find one, then we error that there is an unclosed quote in a field:
                        // We need to scoot from here to the next. so we need an additional storage location.
                        Pos_Quote := Pos_Next;
                        Pos_Next := STRPOS(SubText, In_Quote);
                        IF Pos_Next = 0 THEN ERROR('Error in Column ' + FORMAT(ColNo) + ' Unescaped String found at position ' + FORMAT(Pos));

                        Pos_Next := Pos_Quote + Pos_Next - 1;
                        // Add the Pos_Quote to it so it lines up to the next quote location
                        //Debug_Text := Debug_Text + ' Skipping to Next Quote at ' + FORMAT(Pos_Next) + ';';
                    END;
                end;

                // Then this is the end of quote set as we found a non-escaped quote end
                //Pos_Next := Pos + Pos_Next; // Add the Pos to it so it lines up:
                // Grab INSIDE the Quotes (So After Pos and 2 back from Delimeter)
                Out_Text := COPYSTR(In_Text, Pos + 1, Pos_Next - Pos - 2);

                //Message(Debug_Text);
                //Message('Reading Quoted String ' + FORMAT(Pos) + '->' + FORMAT(Pos_Next) + '=' + Out_Text);
                Out_Array.Add(ColNo, Out_Text);
                // Now escape this loop
                Pos := Pos_Next + 1; // Skip the End Quote and Comma
                Pos_Next := 0;

                //Message(Debug_Text);
                Debug_Text := '';

                // Repair Pos_Next
                Pos_Next := Pos;
            END ELSE BEGIN
                // Then this is a simple string (Or a blank field), just snag, increase, and Dump
                // Grab Next Position (Starting at current subtext Pos):
                Pos_Next := STRPOS(SubText, In_Delimeter);

                // If Pos_Next = 0, then grab the REST of the string:
                IF Pos_Next = 0 THEN begin
                    // Now Set the Current Column Number and Value:
                    Out_Array.Add(ColNo, SubText);
                END ELSE BEGIN
                    // Now Set the Current Column Number and Value:
                    Pos_Next := Pos + Pos_Next; // Add the Pos to it so it lines up:
                    Out_Text := COPYSTR(In_Text, Pos, Pos_Next - Pos - 1);
                    Out_Array.Add(ColNo, Out_Text);
                END;
            end;

            // Increment Column and move positions
            ColNo := ColNo + 1;
            Pos := Pos_Next;
        END;
    end;
}