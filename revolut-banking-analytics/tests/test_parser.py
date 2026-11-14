from banking_pipeline.parser import parse_interleaved_text, InterleavedParseError

def test_parser_is_order_independent():
    text=("HDR|ACCOUNTS\nAccountID|CustomerID\n1|10\nFTR|ACCOUNTS\nHDR|CUSTOMERS\nCustomerID|Name\n10|A\nFTR|CUSTOMERS\n")
    blocks=parse_interleaved_text(text)
    assert set(blocks)=={"ACCOUNTS","CUSTOMERS"}
    assert blocks["CUSTOMERS"].rows==[["10","A"]]

def test_parser_rejects_bad_width():
    text=("HDR|CUSTOMERS\nCustomerID|Name\n1\nFTR|CUSTOMERS\n")
    try:
        parse_interleaved_text(text)
        assert False,"Expected parse error"
    except InterleavedParseError:
        pass
