#!/usr/bin/env python

from optparse import OptionParser, OptionGroup
import sys
import subprocess
import re
from datetime import datetime, date
from elixir import *
from sqlalchemy import asc, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from numpy import std


NUM_EXPECTED_CLI_ARGS = 1
INPUT_FILE_PLACEHOLDER = '%%INPUT_FILE%%'
EXTRACTION_COMMAND_TEMPLATE = ['pdftotext', '-layout', INPUT_FILE_PLACEHOLDER, '-']
VAT_FACTOR = 1.19


class BillingDate(Entity):
    using_options(tablename='billing_date', inheritance='multi')

    date = Field(Date, primary_key=True)
    connections = OneToMany('ConnectionType')

    def __init__(self, billing_date):
        self.date = billing_date
        self.calls = ()

    def __str__(self):
        return str(self.date)


class ConnectionType(Entity):
    FESTNETZ = 'NA'
    NETZEXTERN = 'NX'
    NETZINTERN = 'PI'
    SMS = 'SMS'
    INET = 'GPRS'

    FEES = { FESTNETZ: {'net': 0.00, 'gross': 0.00},
             NETZEXTERN: {'net': 0.00, 'gross': 0.00},
             NETZINTERN: {'net': 0.00, 'gross': 0.00},
             SMS: {'net': 0.00, 'gross': 0.00},
             INET: {'net': 0.00, 'gross': 0.00} }

    # set gross prices
    FEES[FESTNETZ]['gross'] = 0.09
    FEES[NETZEXTERN]['gross'] = 0.09
    FEES[NETZINTERN]['gross'] = 0.09
    FEES[SMS]['gross'] = 0.09
    FEES[INET]['gross'] = 0.49

    # calculate net fees from given gross values
    FEES[FESTNETZ]['net'] = FEES[FESTNETZ]['gross'] / VAT_FACTOR
    FEES[NETZEXTERN]['net'] = FEES[NETZEXTERN]['gross'] / VAT_FACTOR
    FEES[NETZINTERN]['net'] = FEES[NETZINTERN]['gross'] / VAT_FACTOR
    FEES[SMS]['net'] = FEES[SMS]['gross'] / VAT_FACTOR
    FEES[INET]['net'] = FEES[INET]['gross'] / VAT_FACTOR


    using_options(tablename='connection_type', inheritance='multi')

    type_ = Field(String(4), primary_key=True)
    amount = Field(Integer)
    net = Field(Float)
    gross = Field(Float)
    date = ManyToOne('BillingDate', primary_key=True)


    def __init__(self, connection_type):
        self.type_ = connection_type
        self.parse_pattern = ''
        self.amount = 0
        self.net = 0.0
        self.gross = 0.0

    def get_parse_pattern(self):
        return self.parse_pattern


    def add_connections(self, connections):
        for parsed_price in connections:
            self._add_connection(float(parsed_price.replace(',', '.')))


    def _add_connection(self, net_price):
        self.net += net_price
        self.gross += net_price * VAT_FACTOR
        self.amount += int(round(net_price / ConnectionType.FEES[self.type_]['net']))


class Calls(ConnectionType):

    using_options(tablename='calls', inheritance='multi')

    def __init__(self, call_type):
        super(Calls, self).__init__(call_type)
        self.parse_pattern = '%(date)s +%(time)s +%(type)s +%(destNumber)s +%(destProvider)s +%(duration)s +(%(price)s)' % {'date': '\d{2}\.\d{2}\.\d{2}',
                                     'time': '\d{2}:\d{2}:\d{2}',
                                     'type': self.type_,
                                     'destNumber': '\d+',
                                     'destProvider': '\S+',
                                     'duration': '\d+:\d{2}',
                                     'price': '\d+,\d{4}'}


    def __str__(self):
        return u"{0}\t{1} min\t| {2}\u20AC ({3}\u20AC)".format(self.type_,
                                                              self.amount,
                                                              self.net,
                                                              self.gross).encode('utf-8')

class TextMessages(ConnectionType):

    using_options(tablename='text_messages', inheritance='multi')

    def __init__(self):
        super(TextMessages, self).__init__(ConnectionType.SMS)
        self.parse_pattern = '%(date)s +%(time)s +%(type)s +%(destNumber)s +%(destProvider)s +%(quantity)s +(%(price)s)' % {'date': '\d{2}\.\d{2}\.\d{2}',
                                     'time': '\d{2}:\d{2}:\d{2}',
                                     'type': self.type_,
                                     'destNumber': '\d+',
                                     'destProvider': '\S+',
                                     'quantity': '\d+',
                                     'price': '\d+,\d{4}'}


    def __str__(self):
        return u"{0}\t{1} SMS\t| {2}\u20AC ({3}\u20AC)".format(self.type_,
                                                              self.amount,
                                                              self.net,
                                                              self.gross).encode('utf-8')


class MobileWebConnections(ConnectionType):

    using_options(tablename='mobile_web_connections', inheritance='multi')

    # klarmobil charges for 100KB chunks
    INET_CHUNK_SIZE = 100

    def __init__(self):
        super(MobileWebConnections, self).__init__(ConnectionType.INET)
        self.parse_pattern = '%(date)s +%(time)s +%(type)s +%(gateway)s +- +%(duration)s/ +(%(quantity)s) +(%(price)s)' % {'date': '\d{2}\.\d{2}\.\d{2}',
                                     'time': '\d{2}:\d{2}:\d{2}',
                                     'type': self.type_,
                                     'gateway': 'internet.online',
                                     'duration': '\d+:\d{2}',
                                     'quantity': '\d+',
                                     'price': '\d+,\d{4}'}

    def __str__(self):
        return u"{0}\t{1} kB\t| {2}\u20AC ({3}\u20AC)".format(self.type_,
                                                              self.amount,
                                                              self.net,
                                                              self.gross).encode('utf-8')



    def add_connections(self, connections):
            for amount, price in connections:
                self._add_connection(int(amount), float(price.replace(',', '.')))


    def _add_connection(self, amount, net_price):
        self.net += net_price
        self.gross += net_price * VAT_FACTOR
        self.amount += amount + (MobileWebConnections.INET_CHUNK_SIZE - (amount % MobileWebConnections.INET_CHUNK_SIZE))


class InvoiceParser:

    def __init__(self, extracted_text):
        self.invoice = extracted_text

    def extract_rechnungsdatum(self):
        match = re.search('Rechnungsdatum: +(\d{2})\.(\d{2})\.(\d{4})', self.invoice)
        if not match:
            raise LookupError('Could not extract the date of invoice!')
        else:
            return date(int(match.group(3)), int(match.group(2)), int(match.group(1)))


    def extract_connections(self, connection_type):
        '''
        parse the stats for the given connection type from the extracted text
        '''
        connections = re.findall(connection_type.get_parse_pattern(), self.invoice, re.M)
        if not connections:
            raise UserWarning('No connections of type {0}!'.format(connection_type.type_))
        else:
            connection_type.add_connections(connections)


def parse_commandline_parameters(given_params, num_expected_args):
    '''
    Parse and validate the command line parameters
    '''
    parsed_options = None
    parsed_args = []

    # create parser
    prog_usage = '%prog [options] data_base_file'
    prog_description = 'Analyze cell phone invoice data. '\
                       'Extract connection data from a Klarmobil EVN '\
                       '(Einzelverbindungsnachweis) in .pdf format and add it '\
                       'to the given data base to be available for analysis. '\
                       'Query the data base for statistics of the '\
                       'registered connection types, such as average usage of '\
                       'minutes, short messages and data services.'
    cli_parser = OptionParser(usage=prog_usage, description=prog_description)

    # add options
    #   adding data
    add_group = OptionGroup(cli_parser, 'Adding data')
    add_group.add_option('-a', '--add-invoice', dest='invoice_file',
                         metavar='FILE', help='add invoice FILE to the '\
                                              'data base')

    #   analysing data
    analysis_group = OptionGroup(cli_parser, 'Analysing data')
    analysis_group.add_option('-S', '--show-statistics', dest='show_stats',
                           action='store_true', help='display statistics '\
                                                     'calculated over all '\
                                                     'registered connection '\
                                                     'data.')

    #   inspecting data
    inspection_group = OptionGroup(cli_parser, 'Inspecting data')
    inspection_group.add_option('-m', '--get-month', dest='month',
                           metavar='MONTH', help='display the data registered '\
                                                 'for the given MONTH '\
                                                 '[e.g.: \'{0:%Y-%m}\']'.format(datetime.today()))
    inspection_group.add_option('-M', '--get-all-months', dest='all_months',
                           action='store_true', help='display the data for '\
                                                     'all registered billing '\
                                                     'dates')
    inspection_group.add_option('-L', '--list-months', dest='list_months',
                           action='store_true', help='list the dates of all '\
                                                     'registered months')
    #   register groups
    cli_parser.add_option_group(add_group)
    cli_parser.add_option_group(analysis_group)
    cli_parser.add_option_group(inspection_group)

    #   set defaults
    cli_parser.set_defaults(all_months=False)
    cli_parser.set_defaults(list_months=False)
    cli_parser.set_defaults(show_stats=False)

    # parse cli parameters
    parsed_options, parsed_args = cli_parser.parse_args(given_params)

    # validate params
    if len(parsed_args) != num_expected_args:
        cli_parser.error('incorrect number of arguments')
    if parsed_options.month:
        try:
            year = int(parsed_options.month[0:4])
            month = int(parsed_options.month[5:7])
            date_ = date(year, month, 1)
            parsed_options.month = date_
        except Exception as error:
            cli_parser.error('\'{0}\' is not a valid year-month '\
                             'combination: {1}'.format(parsed_options.month,
                                                       error))

    # add mandatory data base to options for improved lookup ability
    parsed_options.data_base = parsed_args[0]

    # return enhanced options as structured params
    return parsed_options




def main():
    '''
    the main function
    '''

    cli_params = None

    # parse cli parameters
    cli_params = parse_commandline_parameters(sys.argv[1:], NUM_EXPECTED_CLI_ARGS)
    input_file = sys.argv[1]

    # connect to data base
    connect_to_db(cli_params.data_base)

    try:
        if cli_params.invoice_file:
            print 'Adding invoice \'{0}\' to data base \'{1}\', '\
                  'ignoring potential querying parameters...'.format(cli_params.invoice_file,
                                                                     cli_params.data_base)
            add_invoice(cli_params.invoice_file)
        elif cli_params.month:
            print 'Fetching data for \'{0:%Y-%m}\'...'.format(cli_params.month)
            get_month(cli_params.month)
        elif cli_params.all_months:
            print 'Fetching data for all months...'
            get_all_months()
        elif cli_params.list_months:
            print 'Fetching data on registered months...'
            list_registered_months()
        elif cli_params.show_stats:
            print 'Calculating statistics...'
            show_connection_stats()
    except SystemExit as signal:
        pass
    finally:
        # clean up
        session.close()


def add_invoice(invoice_file):

    extraction_cmd = []
    extractor = None
    extracted_text = ''
    error_msg = ''

    billing_date = None

    festnetz_calls = None
    netzextern_calls = None
    netzintern_calls = None
    shortmessages = None
    internet_connections = None

    # extract text from .pdf
    #   assemble command
    extraction_cmd = EXTRACTION_COMMAND_TEMPLATE
    extraction_cmd[extraction_cmd.index(INPUT_FILE_PLACEHOLDER)] = invoice_file
    #   execute
    extractor = subprocess.Popen(extraction_cmd,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
    extracted_text, error_msg = extractor.communicate()
    #   handle errors
    if (extractor.returncode != 0) or error_msg:
        print "ERROR: %s" % str(error_msg)
        raise SystemExit(1)

    extractor = InvoiceParser(extracted_text)


    # process text extracted from pdf
    try:
        billing_date = BillingDate(extractor.extract_rechnungsdatum())
    except LookupError as error:
        print 'ERROR: {0}'.format(error)
        raise SystemExit(1)

    # add one instance of each connection type to the current billing date
    billing_date.connections.append(Calls(ConnectionType.FESTNETZ))
    billing_date.connections.append(Calls(ConnectionType.NETZEXTERN))
    billing_date.connections.append(Calls(ConnectionType.NETZINTERN))
    billing_date.connections.append(TextMessages())
    billing_date.connections.append(MobileWebConnections())

    # parse data for each connection type
    for connection_type in billing_date.connections:
        try:
            extractor.extract_connections(connection_type)
        except UserWarning as warning:
            print 'WARNING: {0}'.format(warning)

    # write results to data base
    try:
        session.commit()
    except IntegrityError as error:
        print "ERROR: Could not add new connections to data base: {0}".format(error)
        session.rollback()
        raise SystemExit(1)

    # feed added data back to user
    print 'The following data has been registered for billing date {0}:'.format(billing_date.date)
    for connection_type in billing_date.connections:
        print connection_type



def get_month(month):
    try:
        result = BillingDate.query.filter(BillingDate.date.like('{0:%Y-%m-__}'.format(month))).one()
        for connection_type in result.connections:
            print '   {0}'.format(connection_type)
    except (NoResultFound, MultipleResultsFound) as error:
        print 'ERROR: Could not fetch data for month \'{0:%Y-%m}\' '\
              'from data base: {1}'.format(month, error)
        raise SystemExit(1)


def get_all_months():
    months = BillingDate.query.order_by(asc(BillingDate.date)).all()
    for month in months:
        print "\n{0}:".format(month)
        for connection_type in month.connections:
            print '   {0}'.format(connection_type)

def list_registered_months():
    for month in BillingDate.query.order_by(asc(BillingDate.date)).all():
        print '   {0}'.format(month)

def show_connection_stats():
    # fetch avg, min and max values from data base
    net_external = session.query(func.avg(ConnectionType.amount),
                                 func.min(ConnectionType.amount),
                                 func.max(ConnectionType.amount),
                                 func.avg(ConnectionType.net),
                                 func.avg(ConnectionType.gross)
                                ).filter(ConnectionType.type_==ConnectionType.NETZEXTERN).one()
    net_internal = session.query(func.avg(ConnectionType.amount),
                                 func.min(ConnectionType.amount),
                                 func.max(ConnectionType.amount),
                                 func.avg(ConnectionType.net),
                                 func.avg(ConnectionType.gross)
                                ).filter(ConnectionType.type_==ConnectionType.NETZINTERN).one()
    land_line = session.query(func.avg(ConnectionType.amount),
                              func.min(ConnectionType.amount),
                              func.max(ConnectionType.amount),
                              func.avg(ConnectionType.net),
                              func.avg(ConnectionType.gross)
                             ).filter(ConnectionType.type_==ConnectionType.FESTNETZ).one()
    short_messages = session.query(func.avg(ConnectionType.amount),
                                   func.min(ConnectionType.amount),
                                   func.max(ConnectionType.amount),
                                   func.avg(ConnectionType.net),
                                   func.avg(ConnectionType.gross)
                                  ).filter(ConnectionType.type_==ConnectionType.SMS).one()
    web_connections = session.query(func.avg(MobileWebConnections.amount),
                                    func.min(MobileWebConnections.amount),
                                    func.max(MobileWebConnections.amount),
                                    func.avg(MobileWebConnections.net),
                                    func.avg(MobileWebConnections.gross)
                                   ).filter(ConnectionType.type_==ConnectionType.INET).one()

    # calculate standard deviations that sqlite cannot provice
    net_external_stdev = std(session.query(ConnectionType.amount).filter(ConnectionType.type_==ConnectionType.NETZEXTERN).all())
    net_internal_stdev = std(session.query(ConnectionType.amount).filter(ConnectionType.type_==ConnectionType.NETZINTERN).all())
    land_line_stdev = std(session.query(ConnectionType.amount).filter(ConnectionType.type_==ConnectionType.FESTNETZ).all())
    short_messages_stdev = std(session.query(ConnectionType.amount).filter(ConnectionType.type_==ConnectionType.SMS).all())
    web_connections_stdev = std(session.query(ConnectionType.amount).filter(ConnectionType.type_==ConnectionType.INET).all())


    #len_min = max(len(str(net_external[1])), len(str(net_internal[1])),
    #              len(str(land_line[1])), len(str(short_messages[1])),
    #              len(str(web_connections[1])))

    print u' {0:^20}: {1:^14}   {2:^5}   {3:^12} | {4:>6}\u20AC ({5:>6}\u20AC)'.format('connection type',
                                                                                       'avg',
                                                                                       'stdev',
                                                                                       '(min/max)',
                                                                                       'net',
                                                                                       'gross')
    print u'-'*80
    print u'   {0:18}: {1[0]:>10.2f} min    {2:4}   {3:<12} | {1[3]:>6.2f}\u20AC ({1[4]:>6.2f}\u20AC)'.format('net external calls',
                                                                                                              net_external,
                                                                                                              net_external_stdev,
                                                                                                              '({0[1]}/{0[2]})'.format(net_external))
    print u'   {0:18}: {1[0]:>10.2f} min    {2:4}   {3:<12} | {1[3]:>6.2f}\u20AC ({1[4]:>6.2f}\u20AC)'.format('net internal calls',
                                                                                                              net_internal,
                                                                                                              net_internal_stdev,
                                                                                                              '({0[1]}/{0[2]})'.format(net_internal))
    print u'   {0:18}: {1[0]:>10.2f} min    {2:4}   {3:<12} | {1[3]:>6.2f}\u20AC ({1[4]:>6.2f}\u20AC)'.format('land line calls',
                                                                                                              land_line,
                                                                                                              land_line_stdev,
                                                                                                              '({0[1]}/{0[2]})'.format(land_line))
    print u'   {0:18}: {1[0]:>10.2f} SMS    {2:4}   {3:<12} | {1[3]:>6.2f}\u20AC ({1[4]:>6.2f}\u20AC)'.format('short messages',
                                                                                                              short_messages,
                                                                                                              short_messages_stdev,
                                                                                                              '({0[1]}/{0[2]})'.format(short_messages))
    print u'   {0:18}: {1[0]:>10.2f} kB     {2:4}   {3:<12} | {1[3]:>6.2f}\u20AC ({1[4]:>6.2f}\u20AC)'.format('mobile traffic',
                                                                                                              web_connections,
                                                                                                              web_connections_stdev,
                                                                                                              '({0[1]}/{0[2]})'.format(web_connections))

def connect_to_db(data_base):

    metadata.bind = 'sqlite:///{0}'.format(data_base)

    #DEBUG
    #metadata.bind.echo = True

    # setup data base tables and object mappers
    setup_all(True)


#
# static entry point
#
if __name__ == '__main__':
    main()
