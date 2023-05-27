xmlport 50005 "VersaPay Import"
{
    Caption = 'BoA IREC Payment Import';
    Direction = Import;
    Format = VariableText;
    FieldSeparator = ',';
    FieldDelimiter = '<None>';
    TableSeparator = '<NewLine>';
    UseRequestPage = false;

    schema
    {
        textelement(root)
        {
            tableelement(Buffer; "VersaPay Buffer")
            {
                MinOccurs = Zero;

                textelement(payment_reference) { }
                textelement(invoice_number) { }
                textelement(date) { }
                textelement(amount) { }
                textelement(plan_fee) { }
                textelement(payment_amount) { }
                textelement(payment_transaction_amount) { }
                textelement(payment_transaction_token) { }
                textelement(payment_method) { }
                textelement(payment_from_bank_account) { }
                textelement(payment_from_credit_card) { }
                textelement(payment_institution_name) { }
                textelement(payment_credit_card_brand) { }
                textelement(short_pay_indicator) { }
                textelement(payment_note) { }
                textelement(auto_debit_indicator) { }
                textelement(invoice_balance) { }
                textelement(payment_timestamp) { }
                textelement(invoice_division) { }
                textelement(invoice_division_number) { }
                textelement(invoice_division_name) { }
                textelement(pay_to_bank_account) { }
                textelement(pay_to_bank_account_name) { }
                textelement(customer_identifier) { }
                textelement(customer_name) { }
                textelement(status) { }
                textelement(payment_source) { }
                textelement(payment_code) { }
                textelement(payment_description) { }
                textelement(gateway_authorization_code) { }
                textelement(purchase_order_number) { }
                textelement(ref1) { }
                textelement(ref2) { }
                textelement(ref3) { }
                textelement(short_pay_reason_identifier) { }
                textelement(short_pay_reason) { }
                textelement(dispute_reason_identifier) { }
                textelement(dispute_reason) { }
                textelement(invoice_amount_paid) { }
                textelement(invoice_amount) { }
                textelement(invoice_identifier) { }
                textelement(invoice_date) { }
                textelement(invoice_external_id) { }
                textelement(invoice_currency) { }
                textelement(invoice_purchase_order_number) { }
                textelement(invoice_ref1) { }
                textelement(invoice_ref2) { }
                textelement(invoice_ref3) { }
                textelement(cumulative_customer_amount) { }
                textelement(checkout_token) { }
                textelement(status_reason) { }
                textelement(payment_transaction_fee) { }
                textelement(settlement_date) { }
                textelement(customer_address) { }
                textelement(payor_name) { }
                textelement(payor_phone) { }
                textelement(payor_email) { }
                textelement(cardholder_name) { }
                textelement(fund_token) { }
                textelement(card_expiry_year) { }
                textelement(card_expiry_month) { }
                textelement(order_identifier) { }
                textelement(order_document_type) { }
                textelement(order_number) { }
                textelement(ref4) { }
                textelement(invoice_ref4) { }
                textelement(external_payment_number) { }
                textelement(external_payment_type) { }
                textelement(batch_number) { }
                textelement(batch_amount) { }
                textelement(cross_currency_payment) { }
                textelement(cross_currency_payment_amount) { }
                textelement(cross_currency_payment_currency) { }
                textelement(cross_currency_payment_conversion) { }

                trigger OnBeforeInsertRecord()
                var
                    CLE: Record "Cust. Ledger Entry";
                begin
                    LineNo += 1;
                    if payment_reference = 'payment_reference' then
                        currXMLport.Skip();
                    Clear(Buffer);
                    Buffer."payment_reference" := payment_reference;
                    Buffer."invoice_number" := invoice_number;
                    if Evaluate(Buffer."date", date) then;
                    if Evaluate(Buffer."amount", amount) then;
                    if Evaluate(Buffer."short_pay_indicator", short_pay_indicator) then;
                    Buffer."payment_note" := payment_note;
                    Buffer."invoice_division" := invoice_division;
                    Buffer."customer_identifier" := customer_identifier;
                    Buffer."invoice_purchase_order_number" := invoice_purchase_order_number;
                    if Evaluate(Buffer."payment_transaction_fee", payment_transaction_fee) then;
                    Buffer."Line No." := LineNo;
                end;
            }
        }
    }
    trigger OnInitXmlPort()
    begin
        LineNo := 0;
    end;

    var
        LineNo: Integer;
}