codeunit 50055 "VersaPay"
{
    trigger OnRun()
    var
        Window: Dialog;
    begin
        // This Funtion is just so you can run this from the Job queue to quickly enable.
        FindFiles();
        //khOutFiles();
        Message('Bing!');
    end;

    var
        Buffer: Record "VersaPay Buffer";

    procedure FindFiles()
    var
        VersaSetup: Record "VersaPay Setup";
        Files: Record "Name/Value Buffer" temporary;
        TempBlob: Record TempBlob;
        FileMgt: Codeunit "File Management";
        ImportXML: XmlPort "VersaPay Import";
        ImportInStream: InStream;
    begin
        // This function is meant to scan the Input folder and try and detect a usable input file:
        // First, we'll need to collect some setup information:
        VersaSetup.Get();
        VersaSetup.TestField("In Folder");
        VersaSetup.TestField("File Filter");

        FileMgt.GetServerDirectoryFilesList(Files, VersaSetup."In Folder");
        Files.SetFilter(Name, VersaSetup."File Filter");
        if Files.FindFirst() then
            repeat
                Buffer.DeleteAll(false);
                Clear(ImportXML);
                Clear(TempBlob);
                FileMgt.BLOBImportFromServerFile(TempBlob, Files.Name);
                TempBlob.Blob.CreateInStream(ImportInStream);
                Commit();
                ImportXML.SetSource(ImportInStream);
                ImportXML.Filename(Files.Name);
                ClearLastError();
                if not ImportXML.Import() then begin
                    Clear(ImportInStream);
                    Error(GetLastErrorText());
                end else begin
                    Clear(ImportInStream);
                end;
                InputFile();
            until Files.Next = 0;
        // Archive any files that we didn't process
        Files.SetRange(Name);
        if Files.FindFirst() then
            repeat
                BackupFile(Files.Name);
            until Files.Next = 0;
        Files.SetRange(Name);
    end;

    procedure OutFiles()
    begin
        // This function is to expose/allow outputting all files from the Job queue (by outputting all files)
        OutputFile('CUSTOMER');
        OutputFile('INVOICE');
        //OutputFile('PAYMENT_OUT'); This will be Phase 2
        OutputFile('RECON');
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
        VersaSetup.Get();

        if VersaSetup.Backup then begin
            // Then Backup is enabled:
            Path := FileMgt.GetDirectoryName(Filename);
            BackupPath := VersaSetup."In Folder" + '/Backup';
            BackupFile := BackupPath + '/' + FileMgt.GetFileName(FileName);
            if not FileMgt.ServerDirectoryExists(BackupPath) then begin
                FileMgt.ServerCreateDirectory(BackupPath);
            end;
            // Move the file
            FileMgt.MoveFile(Filename, BackupFile);
        end else begin
            // Then we should just Delete the file.
            FileMgt.DeleteServerFile(Filename);
        end;
    end;

    // Input files are coded by the incoming file (to leave room for future expansion)
    procedure InputFile()
    var
        VersaSetup: Record "VersaPay Setup";
        CompanyMap: Record "VersaPay Company Map";
        Journal: Record "Gen. Journal Line";
        JournalCheck: Record "Gen. Journal Line";
        JournalHeader: Record "Gen. Journal Batch";
        NoSeries: Record "No. Series";
        CustLedgEntry: Record "Cust. Ledger Entry";
        NoSeriesMgt: Codeunit "NoSeriesManagement";
        Customer: Record "Customer";
        Success: Boolean;
        LineNo: Integer;
        CurrentCompany: Text[30];
        RowReference: Text;
        Next_PayLineNo: Integer;
        AppliesToDocType: Option; // after upgrade, set this to Enum "Gen. Journal Document Type"
        MissingCustErr: Label 'Missing Customer %1, Company %2, Import Line %3';
    begin
        // First, we'll need to collect some setup information:
        VersaSetup.Get();
        VersaSetup.TestField("In Folder");
        VersaSetup.TestField("File Filter");
        VersaSetup.TestField("Out Folder");
        VersaSetup.TestField("Payments Journal Template");
        VersaSetup.TestField("Payments Journal Batch");
        VersaSetup.TestField("No. Series Code");
        VersaSetup.TestField("Source Code");

        // Ready to check success
        Success := False;

        // Test that the Journal Batches exist
        CompanyMap.FindSet();
        repeat
            JournalHeader.ChangeCompany(CompanyMap."Company Name");
            JournalHeader.Get(VersaSetup."Payments Journal Template", VersaSetup."Payments Journal Batch");
        until CompanyMap.Next() = 0;
        NoSeries.Get(VersaSetup."No. Series Code");
        Clear(JournalHeader);
        Clear(NoSeries);
        Clear(NoSeriesMgt); // Reset it so it'll count properly from here
        LineNo := 1;
        // Lock the Journal and get the next available number under this batch
        Journal.LockTable();
        // Delete any existing journal lines
        Clear(Journal);
        CompanyMap.FindSet();
        repeat
            Journal.ChangeCompany(CompanyMap."Company Name");
            Journal.SetRange("Journal Template Name", VersaSetup."Payments Journal Template");
            Journal.SetRange("Journal Batch Name", VersaSetup."Payments Journal Batch");
            Journal.DeleteAll(true);
        until CompanyMap.Next() = 0;
        Next_PayLineNo := 10;

        if Buffer.FindSet() then
            repeat
                CurrentCompany := MapCompany(Buffer."invoice_division");
                VersaSetup.ChangeCompany(CurrentCompany);
                VersaSetup.Get();
                Clear(Journal);
                Journal.ChangeCompany(CurrentCompany);
                Journal.Init();
                Journal.Validate("Journal Template Name", VersaSetup."Payments Journal Template");
                Journal.Validate("Journal Batch Name", VersaSetup."Payments Journal Batch");
                Journal.Validate("Line No.", Next_PayLineNo);
                // If this a 'Credit', we have to handle it differently:
                // Credits are identified by the 'payment_method' of 'Credit':
                if Buffer."payment_method" = 'CREDIT' then begin
                    // For Credits, there are key differences:
                    // Credits will import as multi-line instead of single-line depending on if they are 'Applied' or 'Used' in payment_note
                    if Buffer."payment_note" = 'CREDIT (APPLIED)' then begin
                        // This creates the Applied part of a Document
                        // The Applied Marks the INVOICE - so Applies-To Doc Type changes
                        Journal.Validate("Applies-to Doc. Type", Journal."Applies-to Doc. Type"::Invoice);
                    end else begin
                        // This creates the Used part of a Document.
                        // The Used Marks the CREDIT - so Applies-To Doc Type Changes:
                        Journal.Validate("Applies-to Doc. Type", Journal."Applies-to Doc. Type"::"Credit Memo");
                    end;
                    // And there is no Balancing - so we need to match the Document No of the other side, if it has already been placed into the Journal:
                    // (i.e. Look up if there exists an Applied to Document Number referencing this)
                    JournalCheck.Reset();
                    JournalCheck.ChangeCompany(CurrentCompany);
                    JournalCheck.SetRange("Journal Template Name", VersaSetup."Payments Journal Template");
                    JournalCheck.SetRange("Journal Batch Name", VersaSetup."Payments Journal Batch");
                    // They are linked by Reference - which is placed into the External Document Number:
                    JournalCheck.SetRange("External Document No.", Buffer."payment_reference");
                    if JournalCheck.FindLast() then begin
                        // Then we have an existing Document Number - use it here to balance the document:
                        Journal.Validate("Document Type", JournalCheck."Document Type");
                        Journal.Validate("Document No.", JournalCheck."Document No.");
                    end else begin
                        // Then this may be the first one importing in a set - Just calculate one:
                        Journal.Validate("Document Type", Journal."Document Type"::"Credit Memo");
                        Journal."Document No." := NoSeriesMgt.DoGetNextNo(VersaSetup."No. Series Code", WorkDate(), False, False);
                    end;
                end else begin
                    // This means they are not Credits - so Just do the normal Payment stuff:
                    Journal.Validate("Document Type", Journal."Document Type"::Payment);
                    Journal."Document No." := NoSeriesMgt.DoGetNextNo(VersaSetup."No. Series Code", WorkDate(), False, False);
                    Journal.Validate("Applies-to Doc. Type", Journal."Applies-to Doc. Type"::Invoice);
                end;
                Journal.Validate("Bal. Account Type", Journal."Bal. Account Type"::"Bank Account");
                Journal."Bal. Account No." := VersaSetup."Balance Account Bank No.";

                // Imported Fields - Are oddly the same for each type
                Journal.Validate("Posting Date", Buffer."date");
                Customer.ChangeCompany(CurrentCompany);
                if not Customer.Get(Buffer.customer_identifier) then
                    Error(MissingCustErr, Buffer.customer_identifier, CurrentCompany, LineNo);
                Journal.Validate("Account Type", Journal."Account Type"::"Customer");
                Journal."Account No." := Customer."No.";
                Journal.Description := Customer."Name";
                if Buffer."invoice_purchase_order_number" <> '' then
                    Journal.Validate("Description", Buffer."invoice_purchase_order_number");
                Journal.Validate("External Document No.", Buffer."payment_reference");
                Journal.Validate(Amount, -1 * Buffer."amount");

                // Set the type based on the Amount
                if Journal.Amount < 0 then begin
                    Journal."Document Type" := Journal."Document Type"::Payment;
                    AppliesToDocType := Journal."Applies-to Doc. Type"::Invoice;
                end else begin
                    Journal."Document Type" := Journal."Document Type"::Refund;
                    AppliesToDocType := Journal."Applies-to Doc. Type"::"Credit Memo";
                end;
                // Applies-To
                CustLedgEntry.Reset();
                CustLedgEntry.ChangeCompany(CurrentCompany);
                CustLedgEntry.SetRange("Customer No.", Customer."No.");
                CustLedgEntry.SetRange("Document No.", Buffer."invoice_number");
                CustLedgEntry.SetRange("Document Type", AppliesToDocType);
                if not CustLedgEntry.FindFirst() then begin
                    CustLedgEntry.SetRange("Document Type");
                    CustLedgEntry.FindFirst();
                end;
                Journal.Validate("Applies-to Doc. Type", CustLedgEntry."Document Type");
                Journal.Validate("Applies-to Doc. No.", CustLedgEntry."Document No.");

                Journal.Validate("Shortcut Dimension 1 Code", Customer."Global Dimension 1 Code");
                Journal.Validate("Shortcut Dimension 2 Code", Customer."Global Dimension 2 Code");
                Journal.Validate("Source Code", VersaSetup."Source Code");
                Journal.Insert(true);

                // Create a Fee Line by using the journal line we just inserted
                if Buffer."payment_transaction_fee" <> 0 then begin
                    Journal.Validate("Line No.", Next_PayLineNo + 1);
                    Journal.Validate("Account Type", Journal."Account Type"::"Bank Account");
                    Journal."Account No." := VersaSetup."Balance Account Bank No.";
                    Journal.Description := 'ARC CC Fee: ' + Customer."No." + '-' + Buffer."payment_reference";
                    Journal.Validate(Amount, Buffer."payment_transaction_fee");
                    Journal.Validate("Bal. Account Type", Journal."Bal. Account Type"::"G/L Account");
                    Journal.Validate("Bal. Account No.", VersaSetup."Fee Account No.");
                    Journal.Validate("Applies-to Doc. No.", '');
                    Journal.Insert(true);
                end;

                LineNo := LineNo + 1;
                Next_PayLineNo := Next_PayLineNo + 10;

                Success := true;
            // Close the file to end the processing
            // Backup the file if there was success (or delete if backup is off)
            until Buffer.Next() = 0;
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
        VersaSetup.Get();

        // Lets Process based on the type of File:
        case FileType of
            'CUSTOMER':
                begin
                    // Make sure an Out Folder and Delimeter are set:
                    if not ((StrLen(VersaSetup."Out Folder") > 0) or not (StrLen(VersaSetup."File Delimiter") > 0)) then Error('Output Files require an Out Folder and File Delimeter to generate output files.');

                    // Generate a Filename:
                    OutFilename := VersaSetup."Out Folder" + '/customer' + Format(CURRENTDATETIME(), 0, '<Year4><Month,2><Day,2><Hours24><Minutes,2><Seconds,2>') + '.csv';

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
                    Customer.Reset();
                    Customer.SetFilter("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    Customer.SetFilter("No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    Customer.SetFilter("Customer Posting Group", '<>JAPAN');
                    if Customer.FindSet then
                        repeat
                            // Let's Prepare our Export, Adding Fields to this line:
                            CSVBuffer.InsertEntry(LineNo, 1, StrPrep(Customer."No.")); // 'identifier'
                            CSVBuffer.InsertEntry(LineNo, 2, StrPrep(Customer."Bill-to Customer No.")); // 'parent_identifier'
                            CSVBuffer.InsertEntry(LineNo, 3, StrPrep(Customer.Name)); // 'name'
                            CSVBuffer.InsertEntry(LineNo, 4, StrPrep(Customer."Credit Limit (LCY)")); // 'credit_limit_cents'

                            // Payment terms SHOULD be a sub-table, but instead they pulled the 'Code' value and strip the leading N if it exists.  Seriously I couldn't make this up.
                            PaymentTermText := Customer."Payment Terms Code";
                            if CopyStr(PaymentTermText, 1, 1) = 'N' then PaymentTermText := CopyStr(PaymentTermText, 2);
                            CSVBuffer.InsertEntry(LineNo, 5, StrPrep(PaymentTermText)); // 'terms_value' - but with the N stripped off...
                            CSVBuffer.InsertEntry(LineNo, 6, 'Day'); // 'terms_type'

                            // To get the Email Lines, we need to look for TWO kinds of Email.  First, we look for the Invoice and use it if found:
                            // Note: We COULD switch to the Bill-to Customer for this, but since the Jet isn't doing that, I'm not going to yet.
                            // 7: First, use the first Invoicing or Statement one if found:
                            CustomerEmail := '';
                            CustomRepSel.Reset();
                            CustomRepSel.SetRange("Source Type", 18);
                            CustomRepSel.SetRange("Source No.", Customer."No.");
                            CustomRepSel.SetRange("Usage", CustomRepSel."Usage"::"S.Invoice"); // Select for Invoice
                            if CustomRepSel.FindSet() then begin // Customer Type (Table 18)
                                CSVBuffer.InsertEntry(LineNo, 7, StrPrep(CustomRepSel."Send To Email")); // 'contact_email'
                                CustomerEmail := CustomRepSel."Send To Email";
                            end else begin
                                // if it's not found, we attempt to use the STATEMENT email:
                                CustomRepSel.Reset();
                                CustomRepSel.SetRange("Source Type", 18);
                                CustomRepSel.SetRange("Source No.", Customer."No.");
                                CustomRepSel.SetRange("Usage", CustomRepSel."Usage"::"C.Statement"); // Select for Statement
                                if CustomRepSel.FindSet() then begin // Customer Type (Table 18)
                                    // Use Statement if found:
                                    CSVBuffer.InsertEntry(LineNo, 7, StrPrep(CustomRepSel."Send To Email")); // 'contact_email'
                                    CustomerEmail := CustomRepSel."Send To Email";
                                end else begin
                                    // Nothing Found.  Add Empty Field:
                                    CSVBuffer.InsertEntry(LineNo, 7, ''); // 'contact_email'     
                                end;
                            end;
                            // 8: Use the NEXT Invoice or Statement that doesn't match the first one, if we have a first one.
                            if StrLen(CustomerEmail) > 0 then begin
                                CustomRepSel.Reset();
                                CustomRepSel.SetRange("Source Type", 18);
                                CustomRepSel.SetRange("Source No.", Customer."No.");
                                CustomRepSel.SetRange("Usage", CustomRepSel."Usage"::"S.Invoice"); // Select for Invoice
                                CustomRepSel.SetFilter("Send To Email", '<>' + CustomerEmail);
                                if CustomRepSel.FindSet() then begin // Customer Type (Table 18)
                                    CSVBuffer.InsertEntry(LineNo, 8, StrPrep(CustomRepSel."Send To Email")); // 'contact_email'
                                end else begin
                                    // if it's not found, we attempt to use the STATEMENT email:
                                    CustomRepSel.Reset();
                                    CustomRepSel.SetRange("Source Type", 18);
                                    CustomRepSel.SetRange("Source No.", Customer."No.");
                                    CustomRepSel.SetRange("Usage", CustomRepSel."Usage"::"C.Statement"); // Select for Statement
                                    CustomRepSel.SetFilter("Send To Email", '<>' + CustomerEmail);
                                    if CustomRepSel.FindSet() then begin // Customer Type (Table 18)
                                        // Use Statement if found:
                                        CSVBuffer.InsertEntry(LineNo, 8, StrPrep(CustomRepSel."Send To Email")); // 'contact_email'
                                    end else begin
                                        // Nothing Found.  Add Empty Field:
                                        CSVBuffer.InsertEntry(LineNo, 8, ''); // 'contact_email'     
                                    end;
                                end;
                            end else
                                CSVBuffer.InsertEntry(LineNo, 8, ''); // 'contact_email' 

                            CSVBuffer.InsertEntry(LineNo, 9, StrPrep(Customer."Phone No.")); // 'telephone'
                            CSVBuffer.InsertEntry(LineNo, 10, StrPrep(Customer."Fax No.")); // 'Fax'
                            CSVBuffer.InsertEntry(LineNo, 11, StrPrep(Customer.Address)); // 'address_1'
                            CSVBuffer.InsertEntry(LineNo, 12, StrPrep(Customer."Address 2")); // 'address_2'
                            CSVBuffer.InsertEntry(LineNo, 13, StrPrep(Customer.City)); // 'city'
                            CSVBuffer.InsertEntry(LineNo, 14, StrPrep(Customer."Post Code")); // 'postal_code'
                            CSVBuffer.InsertEntry(LineNo, 15, StrPrep(Customer.County)); // (State) 'province'
                            CSVBuffer.InsertEntry(LineNo, 16, StrPrep(Customer."Country/Region Code")); // 'country'
                            CSVBuffer.InsertEntry(LineNo, 17, StrPrep(CompanyName + ';' + Customer."Global Dimension 1 Code" + ';' + Customer."Gen. Bus. Posting Group")); // 'tags'

                            // Increment Line
                            LineNo := LineNo + 1;
                        until Customer.Next = 0;

                    // Write the File:
                    CSVBuffer.SaveData(OutFileName, VersaSetup."File Delimiter");
                end;
            'INVOICE':
                begin
                    // Make sure an Out Folder and Delimeter are set:
                    if not ((StrLen(VersaSetup."Out Folder") > 0) or not (StrLen(VersaSetup."File Delimiter") > 0)) then Error('Output Files require an Out Folder and File Delimeter to generate output files.');

                    // Generate a Filename:
                    OutFilename := VersaSetup."Out Folder" + '/invoice' + Format(CURRENTDATETIME(), 0, '<Year4><Month,2><Day,2><Hours24><Minutes,2><Seconds,2>') + '.csv';

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
                    // and that we're supposed to filter out be remaining - we'll use the ledger entry to lead what should be copied to avoid the issues in the file.
                    // This will also let us cycle ONCE to get the 'AR Beg Bal' lines as well - since they appear to use the same limits.
                    // Part -1: The Dimensions aren't flowing right, so we have to do this by Customer first:
                    Customer.Reset();
                    Customer.SetFilter("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    Customer.SetFilter("No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    Customer.SetFilter("Customer Posting Group", '<>JAPAN');
                    if Customer.FindSet() then
                        repeat
                            // Part 1: Invoices
                            SalesInvoiceHeader.Reset();
                            //SalesInvoiceHeader.SetFilter("Shortcut Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                            SalesInvoiceHeader.SetFilter("Remaining Amount", '<>0');
                            SalesInvoiceHeader.SetFilter("Sell-To Customer No.", Customer."No.");
                            SalesInvoiceHeader.SetFilter("Customer Posting Group", '<>JAPAN');
                            if SalesInvoiceHeader.FindSet then
                                repeat
                                    SalesInvoiceLine.Reset();
                                    SalesInvoiceLine.SetRange("Document No.", SalesInvoiceHeader."No.");
                                    SalesInvoiceLine.SetFilter("Amount", '<>0');
                                    if SalesInvoiceLine.FindSet then
                                        repeat
                                            // We should have everything they need now:
                                            CSVBuffer.InsertEntry(LineNo, 1, StrPrep(SalesInvoiceLine."Document No."));  //number
                                            CSVBuffer.InsertEntry(LineNo, 2, StrPrep(SalesInvoiceLine."No."));  //item_code
                                            CSVBuffer.InsertEntry(LineNo, 3, StrPrep(SalesInvoiceLine."Line No."));  //line_item_number
                                            CSVBuffer.InsertEntry(LineNo, 4, StrPrep(SalesInvoiceLine."Description"));  //line_item_description
                                            CSVBuffer.InsertEntry(LineNo, 5, StrPrep(SalesInvoiceLine."Quantity"));  //line_item_quantity
                                            CSVBuffer.InsertEntry(LineNo, 6, StrPrep(SalesInvoiceLine."Unit Price"));  //line_item_unit_cost
                                            CSVBuffer.InsertEntry(LineNo, 7, StrPrep(SalesInvoiceLine."Unit Price" * SalesInvoiceLIne.Quantity));  //line_item_amount
                                            CSVBuffer.InsertEntry(LineNo, 8, StrPrep(SalesInvoiceLine."Line Discount Amount"));  //line_item_discount_amt
                                            CSVBuffer.InsertEntry(LineNo, 9, StrPrep(SalesInvoiceLine."Amount"));  //line_item_total_amount
                                            SalesInvoiceHeader.CalcFields("Amount", "Amount Including VAT", "Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                                            CSVBuffer.InsertEntry(LineNo, 10, StrPrep(SalesInvoiceHeader."Amount"));  //subtotal
                                            CSVBuffer.InsertEntry(LineNo, 11, StrPrep(SalesInvoiceHeader."Amount Including VAT" - SalesInvoiceHeader."Amount"));  //tax (Difference between these two?)
                                            CSVBuffer.InsertEntry(LineNo, 12, StrPrep(SalesInvoiceHeader."Amount Including VAT"));  //amount
                                            CSVBuffer.InsertEntry(LineNo, 13, StrPrep(SalesInvoiceHeader."Remaining Amount"));  //balance
                                            CSVBuffer.InsertEntry(LineNo, 14, StrPrep(SalesInvoiceLine."Shipment Date"));  //shipment_date
                                            CSVBuffer.InsertEntry(LineNo, 15, StrPrep(SalesInvoiceHeader."External Document No."));  //purchase_order_number
                                            CSVBuffer.InsertEntry(LineNo, 16, StrPrep(SalesInvoiceLine."Order No."));  //sales_order_number
                                            CSVBuffer.InsertEntry(LineNo, 17, StrPrep(SalesInvoiceHeader."Bill-To Customer No."));  //customer_identifier
                                            CSVBuffer.InsertEntry(LineNo, 18, StrPrep(CompanyName));  //division (Setting to Company)
                                            CSVBuffer.InsertEntry(LineNo, 19, StrPrep(SalesInvoiceLine."Posting Date"));  //date
                                            CSVBuffer.InsertEntry(LineNo, 20, StrPrep(SalesInvoiceHeader."Order Date"));  //order_date
                                            CSVBuffer.InsertEntry(LineNo, 21, StrPrep(SalesInvoiceHeader."Due Date"));  //due_date
                                            CSVBuffer.InsertEntry(LineNo, 22, StrPrep(SalesInvoiceHeader."Ship-to Name"));  //shipping_name
                                            CSVBuffer.InsertEntry(LineNo, 23, StrPrep(SalesInvoiceHeader."Ship-to Address"));  //shipping_address_1
                                            CSVBuffer.InsertEntry(LineNo, 24, StrPrep(SalesInvoiceHeader."Ship-to Address 2"));  //shipping_address_2
                                            CSVBuffer.InsertEntry(LineNo, 25, StrPrep(SalesInvoiceHeader."Ship-to City"));  //shipping_city
                                            CSVBuffer.InsertEntry(LineNo, 26, StrPrep(SalesInvoiceHeader."Ship-to Post Code"));  //shipping_postal_code
                                            CSVBuffer.InsertEntry(LineNo, 27, StrPrep(SalesInvoiceHeader."Ship-to County"));  //shipping_province
                                            CSVBuffer.InsertEntry(LineNo, 28, StrPrep(SalesInvoiceHeader."Ship-to Country/Region Code"));  //shipping_country
                                            CSVBuffer.InsertEntry(LineNo, 29, StrPrep(SalesInvoiceHeader."Payment Terms Code"));  //terms
                                            CSVBuffer.InsertEntry(LineNo, 30, StrPrep(SalesInvoiceHeader."Salesperson Code"));  //rep
                                            CSVBuffer.InsertEntry(LineNo, 31, StrPrep(SalesInvoiceHeader."Shipping Agent Code"));  //via
                                            CSVBuffer.InsertEntry(LineNo, 32, StrPrep(SalesInvoiceHeader."Your Reference"));  //your_reference
                                            CSVBuffer.InsertEntry(LineNo, 33, '0.01');  //finance_charge
                                            CSVBuffer.InsertEntry(LineNo, 34, '45');  //nsf_fee
                                            CSVBuffer.InsertEntry(LineNo, 35, '2.5');  //convenience_fee

                                            // Increment Line
                                            LineNo := LineNo + 1;

                                        until SalesInvoiceLine.Next = 0;
                                until SalesInvoiceHeader.Next = 0;
                            // Part 2: Credits
                            CreditHeader.Reset();
                            //CreditHeader.SetFilter("Shortcut Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                            CreditHeader.SetFilter("Remaining Amount", '<>0');
                            CreditHeader.SetFilter("Sell-To Customer No.", Customer."No.");
                            CreditHeader.SetFilter("Customer Posting Group", '<>JAPAN');
                            if CreditHeader.FindSet then
                                repeat
                                    CreditLine.Reset();
                                    CreditLine.SetRange("Document No.", CreditHeader."No.");
                                    CreditLine.SetFilter("Amount", '<>0');
                                    if CreditLine.FindSet then
                                        repeat
                                            // We should have everything they need now:
                                            CSVBuffer.InsertEntry(LineNo, 1, StrPrep(CreditLine."Document No."));  //number
                                            CSVBuffer.InsertEntry(LineNo, 2, StrPrep(CreditLine."No."));  //item_code
                                            CSVBuffer.InsertEntry(LineNo, 3, StrPrep(CreditLine."Line No."));  //line_item_number
                                            CSVBuffer.InsertEntry(LineNo, 4, StrPrep(CreditLine."Description"));  //line_item_description
                                            CSVBuffer.InsertEntry(LineNo, 5, StrPrep(CreditLine."Quantity"));  //line_item_quantity
                                            CSVBuffer.InsertEntry(LineNo, 6, StrPrep(-1 * CreditLine."Unit Price"));  //line_item_unit_cost
                                            CSVBuffer.InsertEntry(LineNo, 7, StrPrep(-1 * (CreditLine."Unit Price" * CreditLine."Quantity")));  //line_item_amount
                                            CSVBuffer.InsertEntry(LineNo, 8, StrPrep(CreditLine."Line Discount Amount"));  //line_item_discount_amt
                                            CSVBuffer.InsertEntry(LineNo, 9, StrPrep(-1 * CreditLine."Amount"));  //line_item_total_amount
                                            CreditHeader.CalcFields("Amount", "Amount Including VAT", "Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                                            CSVBuffer.InsertEntry(LineNo, 10, StrPrep(-1 * CreditHeader."Amount"));  //subtotal
                                            CSVBuffer.InsertEntry(LineNo, 11, StrPrep(-1 * (CreditHeader."Amount Including VAT" - CreditHeader."Amount")));  //tax (Difference between these two?)
                                            CSVBuffer.InsertEntry(LineNo, 12, StrPrep(-1 * CreditHeader."Amount Including VAT"));  //amount
                                            CSVBuffer.InsertEntry(LineNo, 13, StrPrep(CreditHeader."Remaining Amount"));  //balance
                                            CSVBuffer.InsertEntry(LineNo, 14, StrPrep(CreditLine."Shipment Date"));  //shipment_date
                                            CSVBuffer.InsertEntry(LineNo, 15, StrPrep(CreditHeader."External Document No."));  //purchase_order_number
                                            CSVBuffer.InsertEntry(LineNo, 16, '');  //sales_order_number (Blank)
                                            CSVBuffer.InsertEntry(LineNo, 17, StrPrep(CreditHeader."Bill-To Customer No."));  //customer_identifier
                                            CSVBuffer.InsertEntry(LineNo, 18, StrPrep(CompanyName));  //division (Setting to Company)
                                            CSVBuffer.InsertEntry(LineNo, 19, StrPrep(CreditLine."Posting Date"));  //date
                                            CSVBuffer.InsertEntry(LineNo, 20, '');  //order_date (Blank)
                                            CSVBuffer.InsertEntry(LineNo, 21, StrPrep(CreditHeader."Due Date"));  //due_date
                                            CSVBuffer.InsertEntry(LineNo, 22, StrPrep(CreditHeader."Ship-to Name"));  //shipping_name
                                            CSVBuffer.InsertEntry(LineNo, 23, StrPrep(CreditHeader."Ship-to Address"));  //shipping_address_1
                                            CSVBuffer.InsertEntry(LineNo, 24, StrPrep(CreditHeader."Ship-to Address 2"));  //shipping_address_2
                                            CSVBuffer.InsertEntry(LineNo, 25, StrPrep(CreditHeader."Ship-to City"));  //shipping_city
                                            CSVBuffer.InsertEntry(LineNo, 26, StrPrep(CreditHeader."Ship-to Post Code"));  //shipping_postal_code
                                            CSVBuffer.InsertEntry(LineNo, 27, StrPrep(CreditHeader."Ship-to County"));  //shipping_province
                                            CSVBuffer.InsertEntry(LineNo, 28, StrPrep(CreditHeader."Ship-to Country/Region Code"));  //shipping_country
                                            CSVBuffer.InsertEntry(LineNo, 29, StrPrep(CreditHeader."Payment Terms Code"));  //terms
                                            CSVBuffer.InsertEntry(LineNo, 30, StrPrep(CreditHeader."Salesperson Code"));  //rep
                                            CSVBuffer.InsertEntry(LineNo, 31, '');  //via (Blank)
                                            CSVBuffer.InsertEntry(LineNo, 32, StrPrep(CreditHeader."Your Reference"));  //your_reference
                                            CSVBuffer.InsertEntry(LineNo, 33, '');  //finance_charge (Blank Credit only?)
                                            CSVBuffer.InsertEntry(LineNo, 34, '');  //nsf_fee (Blank Credit only?)
                                            CSVBuffer.InsertEntry(LineNo, 35, '');  //convenience_fee (Blank Credit only?)

                                            // Increment Line
                                            LineNo := LineNo + 1;

                                        until CreditLine.Next = 0;
                                until CreditHeader.Next = 0;

                            // Part 3: ... Balances?  Ledger entries?  I'm unsure.
                            CustLedger.Reset();
                            //CustLedger.SetFilter("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                            CustLedger.SetFilter("Remaining Amount", '<>0');
                            CustLedger.SetFilter("Document Type", '<>Refund'); // Requested by Jessica to offset a single Beg Bal that has the 'wrong type' 4/20/2023 - reflexive change in the payment file below.
                            CustLedger.SetFilter("Description", 'AR BEG BALANCE');
                            CustLedger.SetFilter("Sell-To Customer No.", Customer."No.");
                            CustLedger.SetFilter("Customer Posting Group", '<>JAPAN');
                            if CustLedger.FindSet then
                                repeat
                                    CSVBuffer.InsertEntry(LineNo, 1, StrPrep(CustLedger."Document No."));  // 'number'
                                    CSVBuffer.InsertEntry(LineNo, 2, StrPrep(CustLedger."Document Type"));  // 'item_code'
                                    CSVBuffer.InsertEntry(LineNo, 3, '');  // 'line_item_number'
                                    CSVBuffer.InsertEntry(LineNo, 4, StrPrep(CustLedger."Description"));  // 'line_item_description'
                                    CSVBuffer.InsertEntry(LineNo, 5, '1');  // 'line_item_quantity'
                                    CSVBuffer.InsertEntry(LineNo, 6, '0');  // 'line_item_unit_cost'
                                    CustLedger.CalcFields("Amount", "Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                                    CSVBuffer.InsertEntry(LineNo, 7, StrPrep(CustLedger."Amount"));  // 'line_item_amount'
                                    CSVBuffer.InsertEntry(LineNo, 8, '0');  // 'line_item_discount_amt'
                                    CSVBuffer.InsertEntry(LineNo, 9, StrPrep(CustLedger."Amount"));  // 'line_item_total_amount'
                                    CSVBuffer.InsertEntry(LineNo, 10, StrPrep(CustLedger."Amount"));  // 'subtotal'
                                    CSVBuffer.InsertEntry(LineNo, 11, '0');  // 'tax'
                                    CSVBuffer.InsertEntry(LineNo, 12, StrPrep(CustLedger."Amount"));  // 'amount'
                                    CSVBuffer.InsertEntry(LineNo, 13, StrPrep(CustLedger."Remaining Amount"));  // 'balance'
                                    CSVBuffer.InsertEntry(LineNo, 14, '');  // 'shipment_date'
                                    CSVBuffer.InsertEntry(LineNo, 15, '');  // 'purchase_order_number'
                                    CSVBuffer.InsertEntry(LineNo, 16, '');  // 'sales_order_number'
                                    CSVBuffer.InsertEntry(LineNo, 17, StrPrep(CustLedger."Sell-To Customer No."));  // 'customer_identifier'
                                    CSVBuffer.InsertEntry(LineNo, 18, StrPrep(CompanyName));  // 'division'
                                    CSVBuffer.InsertEntry(LineNo, 19, StrPrep(CustLedger."Posting Date"));  // 'date'
                                    CSVBuffer.InsertEntry(LineNo, 20, '');  // 'order_date'
                                    CSVBuffer.InsertEntry(LineNo, 21, StrPrep(CustLedger."Due Date"));  // 'due_date'
                                    CSVBuffer.InsertEntry(LineNo, 22, StrPrep(Customer."Name"));  // 'shipping_name'
                                    CSVBuffer.InsertEntry(LineNo, 23, StrPrep(Customer."Address"));  // 'shipping_address_1'
                                    CSVBuffer.InsertEntry(LineNo, 24, StrPrep(Customer."Address 2"));  // 'shipping_address_2'
                                    CSVBuffer.InsertEntry(LineNo, 25, StrPrep(Customer."City"));  // 'shipping_city'
                                    CSVBuffer.InsertEntry(LineNo, 26, StrPrep(Customer."Post Code"));  // 'shipping_postal_code'
                                    CSVBuffer.InsertEntry(LineNo, 27, StrPrep(Customer."County"));  // 'shipping_province'
                                    CSVBuffer.InsertEntry(LineNo, 28, StrPrep(Customer."Country/Region Code"));  // 'shipping_country'
                                    CSVBuffer.InsertEntry(LineNo, 29, StrPrep(Customer."Payment Terms Code"));  // 'terms'
                                    CSVBuffer.InsertEntry(LineNo, 30, StrPrep(Customer."Salesperson Code"));  // 'rep'
                                    CSVBuffer.InsertEntry(LineNo, 31, StrPrep(Customer."Shipping Agent Code"));  // 'via'
                                    CSVBuffer.InsertEntry(LineNo, 32, '');  // 'your_reference' This is not set in any of the records, so ignoring
                                    CSVBuffer.InsertEntry(LineNo, 33, '0.01');  //finance_charge
                                    CSVBuffer.InsertEntry(LineNo, 34, '45');  //nsf_fee
                                    CSVBuffer.InsertEntry(LineNo, 35, '2.5');  //convenience_fee

                                    // Increment Line
                                    LineNo := LineNo + 1;
                                until CustLedger.Next = 0;
                        until Customer.Next = 0;

                    // Write the File:
                    CSVBuffer.SaveData(OutFileName, VersaSetup."File Delimiter");
                end;
            'PAYMENT_OUT':
                begin
                    // Make sure an Out Folder and Delimeter are set:
                    if not ((StrLen(VersaSetup."Out Folder") > 0) or not (StrLen(VersaSetup."File Delimiter") > 0)) then Error('Output Files require an Out Folder and File Delimeter to generate output files.');

                    // Generate a Filename:
                    OutFilename := VersaSetup."Out Folder" + '/paymentbc' + Format(CURRENTDATETIME(), 0, '<Year4><Month,2><Day,2><Hours24><Minutes,2><Seconds,2>') + '.csv';

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

                    Customer.Reset();
                    Customer.SetFilter("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    Customer.SetFilter("No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    Customer.SetFilter("Customer Posting Group", '<>JAPAN');
                    if Customer.FindSet then
                        repeat
                            CustLedger.Reset();
                            //CustLedger.SetFilter("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                            CustLedger.SetFilter("Remaining Amount", '<>0');
                            // I guess a payment here is anything not sent above?... twice?
                            CustLedger.SetFilter("Document Type", '<>Credit Memo&<>Reminder&<>Finance Charge Memo&<>Invoice');
                            //CustLedger.SetFilter("Description", '<>AR BEG BALANCE');
                            // No move this single item out of alignment per Jessica, we will have to scan all and allow refunds back in.
                            CustLedger.SetFilter("Customer No.", Customer."No.");
                            CustLedger.SetFilter("Customer Posting Group", '<>JAPAN');
                            if CustLedger.FindSet then
                                repeat

                                    // Hide any BEG BALANCE except Refund (Per Jessica 4/20/2023):
                                    if ((UpperCase(CustLedger.Description) <> 'AR BEG BALANCE') or ((CustLedger."Document Type" = CustLedger."Document Type"::Refund) and (UpperCase(CustLedger.Description) = 'AR BEG BALANCE'))) then begin
                                        CSVBuffer.InsertEntry(LineNo, 1, StrPrep(CustLedger."Document No." + ' - ' + Format(CustLedger."Entry No."))); // identifier
                                        CSVBuffer.InsertEntry(LineNo, 2, StrPrep(CustLedger."Posting Date")); // date
                                        CSVBuffer.InsertEntry(LineNo, 3, 'USD'); // currency
                                        CSVBuffer.InsertEntry(LineNo, 4, StrPrep(CustLedger."Customer No.")); // customer_identifier
                                        CSVBuffer.InsertEntry(LineNo, 5, StrPrep(Customer."Name")); // customer_name
                                        CSVBuffer.InsertEntry(LineNo, 6, StrPrep(CompanyName + ' DocType:' + Format(CustLedger."Document Type") + ' Description:' + CustLedger.Description)); // payment_note
                                        CustLedger.CalcFields("Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                                        CSVBuffer.InsertEntry(LineNo, 7, StrPrep(-1 * CustLedger."Remaining Amount")); // payment_total

                                        // Increment Line
                                        LineNo := LineNo + 1;
                                    end;
                                until CustLedger.Next = 0;
                        until Customer.Next = 0;

                    // Write the File:
                    CSVBuffer.SaveData(OutFileName, VersaSetup."File Delimiter");
                end;
            'RECON':
                begin
                    // Make sure an Out Folder and Delimeter are set
                    if not ((StrLen(VersaSetup."Out Folder") > 0) or not (StrLen(VersaSetup."File Delimiter") > 0)) then
                        Error('Output Files require an Out Folder and File Delimeter to generate output files.');

                    // Generate a Filename:
                    OutFilename := VersaSetup."Out Folder" + '/recon' + Format(CURRENTDATETIME(), 0, '<Year4><Month,2><Day,2><Hours24><Minutes,2><Seconds,2>') + '.csv';

                    // Prepare the CSV Buffer:
                    CSVBuffer.DeleteAll;
                    LineNo := 1;

                    // Add a header line:
                    CSVBuffer.InsertEntry(LineNo, 1, 'reconciliation_invoice_number');
                    CSVBuffer.InsertEntry(LineNo, 2, 'balance');
                    CSVBuffer.InsertEntry(LineNo, 3, 'division');

                    // Increment Line
                    LineNo := LineNo + 1;

                    Customer.Reset();
                    Customer.SetFilter("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                    Customer.SetFilter("No.", '<>C11173&<>VC01177'); // One for Marucci, One for Victus.  Hardcoding Customer Exemption?  That's cold blooded.
                    Customer.SetFilter("Customer Posting Group", '<>JAPAN');
                    if Customer.FindSet then
                        repeat
                            CustLedger.Reset();
                            //CustLedger.SetFilter("Global Dimension 1 Code", 'O-GEN|R-DIR-CORP|R-PRO-MLBMILB|R-STO-RETAILAUS|R-STO-RETAILBR|R-STO-RETAILCP|R-STO-RETAILDEN|R-STO-RETAILHOU|R-STO-RETAILHQ|R-STO-RETAILLA|R-STO-RETAILLAF|R-STO-RETAILLV|R-STO-RETAILMW|R-STO-RETAILNO|R-STO-RETAILORL|R-STO-RETAILTUL|R-TMS-BASE|R-TMS-BCOLL|R-TMS-SCOLL|R-TMS-SOFT|UNCLASSIFIED|W-RES-INTL|W-RES-ONLI_CAT|W-RES-REGIONAL|W-RES-TEAMDLR|W-RES-TRAINING');
                            CustLedger.SetFilter("Remaining Amount", '<>0');
                            // I guess a payment here is anything not sent above?... twice?
                            // Removing this filter temporarily
                            CustLedger.SetFilter("Document Type", 'Credit Memo|Reminder|Finance Charge Memo|Invoice');
                            CustLedger.SetFilter("Customer No.", Customer."No.");
                            CustLedger.SetFilter("Customer Posting Group", '<>JAPAN');
                            if CustLedger.FindSet then
                                repeat
                                    CSVBuffer.InsertEntry(LineNo, 1, StrPrep(CustLedger."Document No.")); // 'reconciliation_invoice_number'
                                    CustLedger.CalcFields("Remaining Amount"); // Must Force Calculate these Flowfields in this context:
                                    CSVBuffer.InsertEntry(LineNo, 2, StrPrep(CustLedger."Remaining Amount")); // 'balance'
                                    CSVBuffer.InsertEntry(LineNo, 3, StrPrep(CompanyName)); // 'division'

                                    // Increment Line
                                    LineNo := LineNo + 1;
                                until CustLedger.Next = 0;
                        until Customer.Next = 0;

                    // Write the File:
                    CSVBuffer.SaveData(OutFileName, '"');
                end;
        end;
    end;

    procedure StrPrep(In_Value: Variant) Out_Text: Text
    var
        DateFormatLbl: Label '<Month,2>/<Day,2>/<Year4>';
    begin
        // This function is to prepare the output for the Export.  That means removing commas from numeric values,
        // Making sure the date is in the right format for transmission
        // and enclosing strings in quotes (which is implied by the examples, but not in the descrition)
        case true of
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
            else begin
                // Just spit out normal Formatting to force to Text:
                Out_Text := FORMAT(In_Value);
            end;
        end;
    end;

    procedure MapCompany(PassedVar: Variant): Text[30]
    var
        CompanyMap: Record "VersaPay Company Map";
        MissinMapErr: Label 'Missing VersaPay Company Map for Invoice Division %1.';
    begin
        CompanyMap.Reset();
        CompanyMap.SetRange("VersaPay Invoice Division", PassedVar);
        if not CompanyMap.FindFirst() then
            Error(MissinMapErr, PassedVar);
        exit(CompanyMap."Company Name");
    end;
}