table 50022 "VersaPay Buffer"
{
    fields
    {
        field(1; "payment_reference"; Code[20]) { }
        field(2; "invoice_number"; Code[20]) { }
        field(3; "date"; Date) { }
        field(4; "amount"; Decimal) { }
        field(9; "payment_method"; Code[20]) { }
        field(14; "short_pay_indicator"; Boolean) { }
        field(15; "payment_note"; Code[40]) { }
        field(19; "invoice_division"; Text[30]) { }
        field(24; "customer_identifier"; Code[20]) { }
        field(45; "invoice_purchase_order_number"; Code[30]) { }
        field(52; "payment_transaction_fee"; Decimal) { }
        field(100; "Line No."; Integer) { }

    }
    keys
    {
        key("PK"; "payment_reference", "invoice_number", "Line No.") { }
    }
}