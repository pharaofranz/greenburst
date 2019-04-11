#!/usr/bin/env python
import pika
import uuid
import logging
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter


class GPURpcClient(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare('', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='gpu_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=n)
        while self.response is None:
            self.connection.process_data_events()
        return self.response

if __name__ == "__main__":
    parser=ArgumentParser(description='GPU RPC Server', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Be verbose')
    parser.set_defaults(verbose=False)
    values = parser.parse_args()
    logger = logging.getLogger()

    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if values.verbose:
        logging.basicConfig(level=logging.DEBUG, format=format)
    else:
        logging.basicConfig(level=logging.INFO, format=format)
        logging.getLogger('pika').setLevel(logging.INFO)

    test=str("heimdall -nsamps_gulp 524288 -dm 10 10000 -boxcar_max 128 -cand_sep_dm_trial 200 -cand_sep_time 128 -cand_sep_filter 3 -zap_chans 0 1 -zap_chans 1 2 -zap_chans 2 3 -zap_chans 3 4 -zap_chans 4 5 -zap_chans 5 6 -zap_chans 6 7 -zap_chans 1348 1349 -zap_chans 1349 1350 -zap_chans 1350 1351 -zap_chans 1451 1452 -zap_chans 1452 1453 -zap_chans 1453 1454 -zap_chans 1454 1455 -zap_chans 1455 1456 -zap_chans 1456 1457 -zap_chans 1457 1458 -zap_chans 1458 1459 -zap_chans 1459 1460 -zap_chans 1460 1461 -zap_chans 1461 1462 -zap_chans 1462 1463 -zap_chans 1463 1464 -zap_chans 1464 1465 -zap_chans 1465 1466 -zap_chans 1466 1467 -zap_chans 1467 1468 -zap_chans 1470 1471 -zap_chans 1471 1472 -zap_chans 1472 1473 -zap_chans 1473 1474 -zap_chans 1474 1475 -zap_chans 1477 1478 -zap_chans 1478 1479 -zap_chans 1480 1481 -zap_chans 1481 1482 -zap_chans 1482 1483 -zap_chans 1483 1484 -zap_chans 1523 1524 -zap_chans 1524 1525 -zap_chans 1525 1526 -zap_chans 1533 1534 -zap_chans 1534 1535 -zap_chans 1535 1536 -zap_chans 1536 1537 -zap_chans 1538 1539 -zap_chans 1539 1540 -zap_chans 1540 1541 -zap_chans 1541 1542 -zap_chans 1542 1543 -zap_chans 1543 1544 -zap_chans 1545 1546 -zap_chans 1546 1547 -zap_chans 1547 1548 -zap_chans 1548 1549 -zap_chans 1549 1550 -zap_chans 1550 1551 -zap_chans 1551 1552 -zap_chans 1552 1553 -zap_chans 1553 1554 -zap_chans 1554 1555 -zap_chans 1555 1556 -zap_chans 1556 1557 -zap_chans 1557 1558 -zap_chans 1558 1559 -zap_chans 1559 1560 -zap_chans 1560 1561 -zap_chans 1561 1562 -zap_chans 1562 1563 -zap_chans 1563 1564 -zap_chans 1564 1565 -zap_chans 1566 1567 -zap_chans 1567 1568 -zap_chans 1570 1571 -zap_chans 1571 1572 -zap_chans 1572 1573 -zap_chans 1573 1574 -zap_chans 1574 1575 -zap_chans 1575 1576 -zap_chans 1576 1577 -zap_chans 1577 1578 -zap_chans 1578 1579 -zap_chans 1579 1580 -zap_chans 1580 1581 -zap_chans 1581 1582 -zap_chans 1582 1583 -zap_chans 1583 1584 -zap_chans 1584 1585 -zap_chans 1585 1586 -zap_chans 1586 1587 -zap_chans 1588 1589 -zap_chans 1589 1590 -zap_chans 1590 1591 -zap_chans 1591 1592 -zap_chans 1592 1593 -zap_chans 1593 1594 -zap_chans 1594 1595 -zap_chans 1595 1596 -zap_chans 1597 1598 -zap_chans 1598 1599 -zap_chans 1600 1601 -zap_chans 1601 1602 -zap_chans 1603 1604 -zap_chans 1604 1605 -zap_chans 1605 1606 -zap_chans 1606 1607 -zap_chans 1607 1608 -zap_chans 1608 1609 -zap_chans 1609 1610 -zap_chans 1610 1611 -zap_chans 1612 1613 -zap_chans 1613 1614 -zap_chans 1614 1615 -zap_chans 1615 1616 -zap_chans 1616 1617 -zap_chans 1618 1619 -zap_chans 1619 1620 -zap_chans 1620 1621 -zap_chans 1621 1622 -zap_chans 1622 1623 -zap_chans 1625 1626 -zap_chans 1629 1630 -zap_chans 1630 1631 -zap_chans 1631 1632 -zap_chans 1632 1633 -zap_chans 1633 1634 -zap_chans 1636 1637 -zap_chans 1637 1638 -zap_chans 1638 1639 -zap_chans 1639 1640 -zap_chans 1640 1641 -zap_chans 1641 1642 -zap_chans 1642 1643 -zap_chans 1645 1646 -zap_chans 1646 1647 -zap_chans 1647 1648 -zap_chans 1648 1649 -zap_chans 1649 1650 -zap_chans 1651 1652 -zap_chans 1652 1653 -zap_chans 1653 1654 -zap_chans 1654 1655 -zap_chans 1655 1656 -zap_chans 1657 1658 -zap_chans 1658 1659 -zap_chans 1659 1660 -zap_chans 1660 1661 -zap_chans 1661 1662 -zap_chans 1665 1666 -zap_chans 1666 1667 -zap_chans 1667 1668 -zap_chans 1668 1669 -zap_chans 1669 1670 -zap_chans 1670 1671 -zap_chans 1672 1673 -zap_chans 1674 1675 -zap_chans 1675 1676 -zap_chans 1676 1677 -zap_chans 1677 1678 -zap_chans 1678 1679 -zap_chans 1679 1680 -zap_chans 1680 1681 -zap_chans 1681 1682 -zap_chans 1682 1683 -zap_chans 1683 1684 -zap_chans 1684 1685 -zap_chans 1685 1686 -zap_chans 1686 1687 -zap_chans 1687 1688 -zap_chans 2008 2009 -zap_chans 2009 2010 -zap_chans 2054 2055 -zap_chans 2055 2056 -zap_chans 2758 2759 -zap_chans 2792 2793 -zap_chans 2806 2807 -zap_chans 2855 2856 -zap_chans 2856 2857 -zap_chans 2857 2858 -zap_chans 2869 2870 -zap_chans 2870 2871 -zap_chans 2943 2944 -zap_chans 2944 2945 -zap_chans 2945 2946 -zap_chans 2946 2947 -zap_chans 2958 2959 -zap_chans 3064 3065 -zap_chans 3065 3066 -zap_chans 3066 3067 -zap_chans 3680 3681 -zap_chans 3696 3697 -zap_chans 3702 3703 -zap_chans 3704 3705 -zap_chans 3710 3711 -zap_chans 3712 3713 -zap_chans 3718 3719 -zap_chans 3720 3721 -zap_chans 3734 3735 -zap_chans 3735 3736 -zap_chans 3736 3737 -zap_chans 3741 3742 -zap_chans 3742 3743 -zap_chans 3744 3745 -zap_chans 3745 3746 -zap_chans 3749 3750 -zap_chans 3750 3751 -zap_chans 3752 3753 -zap_chans 3753 3754 -zap_chans 3758 3759 -zap_chans 3760 3761 -zap_chans 3761 3762 -zap_chans 3766 3767 -zap_chans 4088 4089 -zap_chans 4089 4090 -zap_chans 4090 4091 -zap_chans 4091 4092 -zap_chans 4092 4093 -zap_chans 4093 4094 -zap_chans 4094 4095 -zap_chans 4095 4096  -output_dir /ldata/trunk/data_2019-01-20_10-27-50/ -f /ldata/trunk/data_2019-01-20_10-27-50/data_2019-01-20_10-27-50.fil")


    gpu_rpc = GPURpcClient()
    response = gpu_rpc.call(test)
    logging.info('Got following response:')
    logging.info(f'{response}')
