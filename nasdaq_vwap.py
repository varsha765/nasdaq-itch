# Parser for NASDAQ TotalView ITCH 5.0 file and calculate the VWAP

import gzip
import shutil
from collections import defaultdict
from pathlib import Path
from urllib.request import urlretrieve
from urllib.parse import urljoin
import struct


# get the TotalView ITCH file.
# Check if already exists in the itchdir path
# Else download it from the source url and store it in itch_path
# unzip the downloaded file to a binary source file

def get_total_view_itch_file(url):
    if not itch_path.exists():
        print('Creating directory')
        itch_path.mkdir()
    else:
        print('Directory exists')

    filename = itch_path / url.split('/')[-1]
    if not filename.exists():
        print('Downloading...', url)
        urlretrieve(url, filename)
    else:
        print('File exists')

    unzipped = itch_path / (filename.stem + '.bin')
    if not unzipped.exists():
        print('Unzipping to', unzipped)
        with gzip.open(str(filename), 'rb') as f_in:
            with open(unzipped, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    else:
        print('File already unpacked')
    return unzipped


# format time utility function to convert the timestamp from nanoseconds to HH:MM:SS format

def format_time(t):
    m, s = divmod(t, 60)
    h, m = divmod(m, 60)
    return f'{h:0>2.0f}:{m:0>2.0f}:{s:0>5.2f}'


# to parse system even messages (S)

def parse_system_event_message(msg_data):
    msg_type = msg_data[0:1].decode('utf-8')
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    event_code = msg_data[11:12].decode('utf-8')

    event_codes_map = {
        'O': 'Start of Messages',
        'S': 'Start of System Hours',
        'Q': 'Start of Market Hours',
        'M': 'End of Market Hours',
        'E': 'End of System Hours',
        'C': 'End of Messages'
    }

    event_description = event_codes_map.get(event_code, 'Unknown')

    return {
        'msg_type': msg_type,
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'event_code': event_code,
        'event_description': event_description
    }


# to parse stock directory message (R)

def parse_stock_directory_message(msg_data):
    msg_type = msg_data[0:1].decode('utf-8')
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    stock = msg_data[11:19].decode('utf-8').strip()
    market_category = msg_data[19:20].decode('utf-8')
    financial_status_indicator = msg_data[20:21].decode('utf-8')
    round_lot_size = struct.unpack(">I", msg_data[21:25])[0]
    round_lots_only = msg_data[25:26].decode('utf-8')
    issue_classification = msg_data[26:27].decode('utf-8')
    issue_subtype = msg_data[27:29].decode('utf-8')
    authenticity = msg_data[29:30].decode('utf-8')
    short_sale_threshold_indicator = msg_data[30:31].decode('utf-8')
    ipo_flag = msg_data[31:32].decode('utf-8')
    luld_reference_price_tier = msg_data[32:33].decode('utf-8')
    etp_flag = msg_data[33:34].decode('utf-8')
    etp_leverage_factor = struct.unpack(">I", msg_data[34:38])[0]
    inverse_indicator = msg_data[38:39].decode('utf-8')

    return {
        'msg_type': msg_type,
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'stock': stock,
        'market_category': market_category,
        'financial_status_indicator': financial_status_indicator,
        'round_lot_size': round_lot_size,
        'round_lots_only': round_lots_only,
        'issue_classification': issue_classification,
        'issue_subtype': issue_subtype,
        'authenticity': authenticity,
        'short_sale_threshold_indicator': short_sale_threshold_indicator,
        'ipo_flag': ipo_flag,
        'luld_reference_price_tier': luld_reference_price_tier,
        'etp_flag': etp_flag,
        'etp_leverage_factor': etp_leverage_factor,
        'inverse_indicator': inverse_indicator
    }


# to parse stock trading action message (H)

def parse_stock_trading_action_message(msg_data):
    msg_type = chr(msg_data[0])
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    stock = msg_data[11:19].decode().strip()
    trading_state = chr(msg_data[19])
    reserved = chr(msg_data[20])
    reason = msg_data[21:25].decode().strip()

    return {
        "Message Type": msg_type,
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Stock": stock,
        "Trading State": trading_state,
        "Reserved": reserved,
        "Reason": reason
    }


# to parse reg sho short sale messafe (Y)

def parse_reg_sho_short_sale_message(msg_data):
    msg_type = chr(msg_data[0])
    locate_code = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    stock = msg_data[11:19].decode().strip()
    reg_sho_action = chr(msg_data[19])

    return {
        "Message Type": msg_type,
        "Locate Code": locate_code,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Stock": stock,
        "Reg SHO Action": reg_sho_action
    }


# to parse market participant position message (L)

def parse_market_participant_position_message(msg_data):
    msg_type = msg_data[0]
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    mpid = msg_data[11:15].decode().strip()
    stock = msg_data[15:23].decode().strip()
    primary_market_maker = chr(msg_data[23])
    market_maker_mode = chr(msg_data[24])
    market_participant_state = chr(msg_data[25])

    parsed_data = {
        "Message Type": chr(msg_type),
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "MPID": mpid,
        "Stock": stock,
        "Primary Market Maker": primary_market_maker,
        "Market Maker Mode": market_maker_mode,
        "Market Participant State": market_participant_state
    }

    return parsed_data


# to parse market wide circuit breaker message (V)

def parse_market_wide_circuit_breaker_message(msg_data):
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    level1 = struct.unpack("<d", msg_data[11:19])[0]
    level2 = struct.unpack("<d", msg_data[19:27])[0]
    level3 = struct.unpack("<d", msg_data[27:35])[0]

    return {
        "Message Type": "V",
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "MWCB Level 1 Value": level1,
        "MWCB Level 2 Value": level2,
        "MWCB Level 3 Value": level3
    }


# to parse market wide circuit breaker status message (W)

def parse_market_wide_circuit_breaker_status_message(msg_data):
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    breached_level = chr(msg_data[11])

    return {
        "Message Type": "W",
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Breached Level": breached_level
    }


# to parse ipo quoting period update message (K)

def parse_ipo_quoting_period_update_message(msg_data):
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    stock = msg_data[11:19].decode().strip()
    ipo_quotation_release_time = struct.unpack(">I", msg_data[19:23])[0]
    ipo_quotation_release_qualifier = chr(msg_data[23])
    ipo_price = struct.unpack(">I", msg_data[24:28])[0] / 10000.0

    return {
        "Message Type": "K",
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Stock": stock,
        "IPO Quotation Release Time": ipo_quotation_release_time,
        "IPO Quotation Release Qualifier": ipo_quotation_release_qualifier,
        "IPO Price": ipo_price
    }


# to parse luld auction collar message (J)

def parse_luld_auction_collar_message(msg_data):
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    stock = msg_data[11:19].decode().strip()
    auction_collar_reference_price = struct.unpack(">I", msg_data[19:23])[0] / 10000.0
    upper_auction_collar_price = struct.unpack(">I", msg_data[23:27])[0] / 10000.0
    lower_auction_collar_price = struct.unpack(">I", msg_data[27:31])[0] / 10000.0
    auction_collar_extension = struct.unpack(">I", msg_data[31:35])[0]

    return {
        "Message Type": "J",
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Stock": stock,
        "Auction Collar Reference Price": auction_collar_reference_price,
        "Upper Auction Collar Price": upper_auction_collar_price,
        "Lower Auction Collar Price": lower_auction_collar_price,
        "Auction Collar Extension": auction_collar_extension
    }


# to parse operational halt message (h)

def parse_operational_halt_message(msg_data):
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    stock = msg_data[13:21].decode().strip()
    market_code = chr(msg_data[21])
    operational_halt_action = chr(msg_data[22])

    return {
        "Message Type": "h",
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Stock": stock,
        "Market Code": market_code,
        "Operational Halt Action": operational_halt_action
    }


# to parse add order message (A)

def parse_add_order_message(msg_data):
    msg_type = chr(msg_data[0])
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    order_reference_number = struct.unpack(">Q", msg_data[11:19])[0]
    buy_sell_indicator = chr(msg_data[19])
    shares = struct.unpack(">I", msg_data[20:24])[0]
    stock = msg_data[24:32].decode().strip()
    price = struct.unpack(">I", msg_data[32:36])[0] / 10000.0

    return {
        "Message Type": msg_type,
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Order Reference Number": order_reference_number,
        "Buy/Sell Indicator": buy_sell_indicator,
        "Shares": shares,
        "Stock": stock,
        "Price": price
    }


# to parse add order mpid message (F)

def parse_add_order_mpid_message(msg_data):
    msg_type = chr(msg_data[0])
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    order_reference_number = struct.unpack(">Q", msg_data[11:19])[0]
    buy_sell_indicator = chr(msg_data[19])
    shares = struct.unpack(">I", msg_data[20:24])[0]
    stock = msg_data[24:32].decode().strip()
    price = struct.unpack(">I", msg_data[32:36])[0] / 10000.0
    attribution = msg_data[36:40].decode()

    return {
        "Message Type": msg_type,
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Order Reference Number": order_reference_number,
        "Buy/Sell Indicator": buy_sell_indicator,
        "Shares": shares,
        "Stock": stock,
        "Price": price,
        "Attribution": attribution
    }


# to parse order exucuted message (E)

def parse_order_executed_message(msg_data):
    msg_type = chr(msg_data[0])
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    order_reference_number = struct.unpack(">Q", msg_data[11:19])[0]
    executed_shares = struct.unpack(">I", msg_data[19:23])[0]
    match_number = struct.unpack(">Q", msg_data[23:31])[0]

    return {
        "Message Type": msg_type,
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Order Reference Number": order_reference_number,
        "Executed Shares": executed_shares,
        "Match Number": match_number
    }


# to parse order executed with price message (C)

def parse_order_executed_with_price_message(msg_data):
    msg_type = chr(msg_data[0])
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    order_reference_number = struct.unpack(">Q", msg_data[11:19])[0]
    executed_shares = struct.unpack(">I", msg_data[19:23])[0]
    match_number = struct.unpack(">Q", msg_data[23:31])[0]
    printable = chr(msg_data[31])
    execution_price = struct.unpack(">I", msg_data[32:36])[0] / 10000.0

    return {
        "Message Type": msg_type,
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Order Reference Number": order_reference_number,
        "Executed Shares": executed_shares,
        "Match Number": match_number,
        "Printable": printable,
        "Execution Price": execution_price
    }


# to parse order cancel message (X)

def parse_order_cancel_message(msg_data):
    msg_type = chr(msg_data[0])
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    order_reference_number = struct.unpack(">Q", msg_data[11:19])[0]
    cancelled_shares = struct.unpack(">I", msg_data[19:23])[0]

    return {
        "Message Type": msg_type,
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Order Reference Number": order_reference_number,
        "Cancelled Shares": cancelled_shares
    }


# to parse order delete message (D)

def parse_order_delete_message(msg_data):
    msg_type = chr(msg_data[0])
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    order_reference_number = struct.unpack(">Q", msg_data[11:19])[0]

    return {
        "Message Type": msg_type,
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Order Reference Number": order_reference_number
    }


# to parse order replace message (U)

def parse_order_replace_message(msg_data):
    msg_type = chr(msg_data[0])
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    original_order_reference_number = struct.unpack(">Q", msg_data[11:19])[0]
    new_order_reference_number = struct.unpack(">Q", msg_data[19:27])[0]
    shares = struct.unpack(">I", msg_data[27:31])[0]
    price = struct.unpack(">I", msg_data[31:35])[0] / 10000.0  # Convert to decimal price

    return {
        "Message Type": msg_type,
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Original Order Reference Number": original_order_reference_number,
        "New Order Reference Number": new_order_reference_number,
        "Shares": shares,
        "Price": price
    }


# to parse trade message (P)

def parse_trade_message(msg_data):
    msg_type = chr(msg_data[0])
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    order_reference_number = struct.unpack(">Q", msg_data[11:19])[0]
    buy_sell_indicator = chr(msg_data[19])
    shares = struct.unpack(">I", msg_data[20:24])[0]
    stock = msg_data[24:32].decode().strip()
    price = struct.unpack(">I", msg_data[32:36])[0] / 10000.0  # Convert to decimal price
    match_number = struct.unpack(">Q", msg_data[36:44])[0]

    return {
        "Message Type": msg_type,
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Order Reference Number": order_reference_number,
        "Buy/Sell Indicator": buy_sell_indicator,
        "Shares": shares,
        "Stock": stock,
        "Price": price,
        "Match Number": match_number
    }


# to parse broken trade message (B)

def parse_broken_trade_message(msg_data):
    msg_type = chr(msg_data[0])
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    match_number = struct.unpack(">Q", msg_data[11:19])[0]

    return {
        "Message Type": msg_type,
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Match Number": match_number
    }


# to parse cross trade message (Q)

def parse_cross_trade_message(msg_data):
    msg_type = chr(msg_data[0])
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    shares = struct.unpack(">Q", msg_data[11:19])[0]
    stock = msg_data[19:27].decode().strip()
    cross_price = struct.unpack(">I", msg_data[27:31])[0] / 10000.0  # Convert to decimal price
    match_number = struct.unpack(">Q", msg_data[31:39])[0]
    cross_type = chr(msg_data[39])

    return {
        "Message Type": msg_type,
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Shares": shares,
        "Stock": stock,
        "Cross Price": cross_price,
        "Match Number": match_number,
        "Cross Type": cross_type
    }


# to parse noii message (I)

def parse_noii_message(msg_data):
    msg_type = chr(msg_data[0])
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    paired_shares = struct.unpack(">Q", msg_data[11:19])[0]
    imbalance_shares = struct.unpack(">Q", msg_data[19:27])[0]
    imbalance_direction = chr(msg_data[27])
    stock = msg_data[28:36].decode().strip()
    far_price = struct.unpack(">I", msg_data[36:40])[0] / 10000.0
    near_price = struct.unpack(">I", msg_data[40:44])[0] / 10000.0
    current_reference_price = struct.unpack(">I", msg_data[44:48])[0] / 10000.0
    cross_type = chr(msg_data[48])
    price_variation_indicator = chr(msg_data[49])

    return {
        "Message Type": msg_type,
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Paired Shares": paired_shares,
        "Imbalance Shares": imbalance_shares,
        "Imbalance Direction": imbalance_direction,
        "Stock": stock,
        "Far Price": far_price,
        "Near Price": near_price,
        "Current Reference Price": current_reference_price,
        "Cross Type": cross_type,
        "Price Variation Indicator": price_variation_indicator
    }


# to parse retail intersect message (N)

def parse_retail_interest_message(msg_data):
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    stock = msg_data[11:19].decode().strip()
    interest_flag = chr(msg_data[19])

    return {
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Stock": stock,
        "Interest Flag": interest_flag,
    }


# to parse direct listing with capital raise message (O)

def parse_direct_listing_with_capital_raise_message(msg_data):
    stock_locate = struct.unpack(">H", msg_data[1:3])[0]
    tracking_number = struct.unpack(">H", msg_data[3:5])[0]
    seconds = int.from_bytes(msg_data[5:11], byteorder='big') * 1e-9
    timestamp = format_time(seconds)
    stock = msg_data[11:19].decode().strip()
    open_eligibility_status = chr(msg_data[19])
    minimum_allowable_price = struct.unpack(">I", msg_data[20:24])[0] / 10000.0
    maximum_allowable_price = struct.unpack(">I", msg_data[24:28])[0] / 10000.0
    near_execution_price = struct.unpack(">I", msg_data[28:32])[0] / 10000.0
    near_execution_time = struct.unpack("<Q", msg_data[32:40])[0]
    lower_price_range_collar = struct.unpack(">I", msg_data[40:44])[0] / 10000.0
    upper_price_range_collar = struct.unpack(">I", msg_data[44:48])[0] / 10000.0

    return {
        "Message Type": "O",
        "Stock Locate": stock_locate,
        "Tracking Number": tracking_number,
        "Timestamp": timestamp,
        "Stock": stock,
        "Open Eligibility Status": open_eligibility_status,
        "Minimum Allowable Price": minimum_allowable_price,
        "Maximum Allowable Price": maximum_allowable_price,
        "Near Execution Price": near_execution_price,
        "Near Execution Time": near_execution_time,
        "Lower Price Range Collar": lower_price_range_collar,
        "Upper Price Range Collar": upper_price_range_collar
    }


# map of message type and parser

message_parser = {'S': parse_system_event_message,
                  'R': parse_stock_directory_message,
                  'H': parse_stock_trading_action_message,
                  'Y': parse_reg_sho_short_sale_message,
                  'L': parse_market_participant_position_message,
                  'V': parse_market_wide_circuit_breaker_message,
                  'W': parse_market_wide_circuit_breaker_status_message,
                  'K': parse_ipo_quoting_period_update_message,
                  'J': parse_luld_auction_collar_message,
                  'h': parse_operational_halt_message,
                  'A': parse_add_order_message,
                  'F': parse_add_order_mpid_message,
                  'E': parse_order_executed_message,
                  'C': parse_order_executed_with_price_message,
                  'X': parse_order_cancel_message,
                  'D': parse_order_delete_message,
                  'U': parse_order_replace_message,
                  'P': parse_trade_message,
                  'B': parse_broken_trade_message,
                  'Q': parse_cross_trade_message,
                  'I': parse_noii_message,
                  'N': parse_retail_interest_message,
                  'O': parse_direct_listing_with_capital_raise_message
                  }

trade_execution_msgs = ['E', 'C', 'P', 'Q']
order_msgs = ['A', 'F']
system_event_msg = ['S']


# file parser
def parse_file(file_name):
    curr_hour = None

    with open(file_name, 'rb') as file:
        while True:

            # Read the first 2 bytes to get the message size
            msg_size_bytes = file.read(2)
            if not msg_size_bytes:
                break

            # Convert the 2 bytes into an integer to get the message size
            msg_size = struct.unpack(">H", msg_size_bytes)[0]

            # Read the message content based on the message size
            msg_data = file.read(msg_size)

            # Process the message based on the message type (first byte of the message)
            msg_type = chr(msg_data[0])
            parsed_message = message_parser[msg_type](msg_data)

            if not parsed_message:
                continue

            counter[msg_type] += 1
            count['total'] += 1
            if count['total'] % 10000000 == 0:
                print('***** count of messages parsed ***** ', count['total'])

            if msg_type in system_event_msg:
                if parsed_message['event_code'] == 'M':
                    print('vwap at market close ', vwap)
                    continue

            elif msg_type in order_msgs:
                current_id = parsed_message['Order Reference Number']
                order_id[current_id] = {}
                order_id[current_id]['stock'] = parsed_message['Stock']
                order_id[current_id]['shares'] = parsed_message['Shares']
                order_id[current_id]['price'] = parsed_message['Price']

            elif msg_type in trade_execution_msgs:
                stock, price, shares = None, None, None
                if msg_type == 'E':
                    current_id = parsed_message['Order Reference Number']
                    if current_id not in order_id:
                        continue
                    stock, price, shares = order_id[current_id]['stock'], order_id[current_id]['price'], parsed_message[
                        'Executed Shares']
                elif msg_type == 'C':
                    current_id = parsed_message['Order Reference Number']
                    if current_id not in order_id:
                        continue
                    stock, price, shares = order_id[current_id]['stock'], parsed_message['Execution Price'], \
                                           parsed_message['Executed Shares']
                elif msg_type == 'P':
                    stock, price, shares = parsed_message['Stock'], parsed_message['Price'], parsed_message['Shares']
                elif msg_type == 'Q':
                    stock, price, shares = parsed_message['Stock'], parsed_message['Cross Price'], parsed_message[
                        'Shares']
                if curr_hour is None:
                    curr_hour = parsed_message['Timestamp'][:2]
                key = stock
                if key not in stock_map:
                    stock_map[key] = {}
                    stock_map[key]['cum_price_vol'] = 0
                    stock_map[key]['vol'] = 0
                    vwap[key] = []

                curr_price = price
                cum_price['curr_stock'] += curr_price
                total_vol['curr_stock']+=curr_vol
                curr_vol = shares
                stock_map[key]['cum_price_vol'] += (curr_price * curr_vol)
                stock_map[key]['vol'] += curr_vol
                avg_price['curr_stock'] = cum_price['curr_stock']/num_of_transactions

                #avg price

                if stock_map[key]['vol'] != 0:
                    vwap[key] = (round(stock_map[key]['cum_price_vol'] / stock_map[key]['vol'], 4))

            if curr_hour is not None and 'Timestamp' in parsed_message and curr_hour != parsed_message['Timestamp'][:2]:
                print('vwap at ', curr_hour, vwap)
                curr_hour = parsed_message['Timestamp'][:2]


if __name__ == "__main__":
    itch_path = Path('itchdir')
    SOURCE_URL = 'https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/'
    SOURCE_FILE = '01302019.NASDAQ_ITCH50.gz'
    binary_itch_file = get_total_view_itch_file(urljoin(SOURCE_URL, SOURCE_FILE))
    counter = defaultdict(int)
    order_id = {}
    count = {'total': 0}
    stock_map = {}
    vwap = {}
    parse_file(binary_itch_file)
    print('count ', count)
    print('counter ', counter)



