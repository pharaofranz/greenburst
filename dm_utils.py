#!/usr/bin/env python3

from subprocess import PIPE, Popen, check_output
import sys

isPulsar = False  # Is used to check if the source is a pulsar or not.

# Until I figure out how to send a message around that contains several arguments
# I hardcode the FRB DMs here and have a function for pulsars below
def get_dm(src):
    global isPulsar
    FRB_DMs = {'R1': 557.0,
               'FRB': 557.0,
               'INBEAM': 557.0,
               'R2': 190.0,
               'R3': 349.7,
               'R4': 103.0,
               'R5': 450.0,
               'R6': 363.5,
               'R7': 444.0,
               'R8': 1281.5,
               'R10': 424.9,
               'R9': 309.6,
               'R11': 460.2,
               'R12': 578.9,
               'R13': 552.7,
               'R14': 301.7,
               'R15': 195.8,
               'R16': 394.2,
               'R17': 223.7,
               'R18': 1379.0,
               'FRB190417': 1379.0,
               'R19': 490.0,
               'R21': 714.0,
               'R24': 400.0,
               'R25': 222.0,
               'R34': 325.0,
               'R47': 365.0,
               'R48': 625.0,
               'R54': 220.0,
               'R65': 1705.0,
               'R68': 415.0,
               'R70': 290.0,
               'R74': 510.0,
               'NR1': 277.0,
               'NR2': 187.0,
               'NR3': 764.0,
               'NR4': 183.0,
               'NR5': 443.0,
               'NR6': 597.0,
               'NR7': 251.0,
               'SGR1935': 332.7,
               'SGR': 332.7,
               'BSGR': 332.7,
               'FRB180301': 517.0,
               'R180301': 517.0,
               'FRB20180901A': 517.0,
               'R200616': 977.90,
               'FRB190608': 338.7,
               'M81': 88.0,
               'M81R': 88.0,
               'R200120': 88.0,
               'FRB20200120': 88.0,
               'LSI63': 241.0,
               'LS63': 241.0,
               'LSI61': 241.0,
               'R67': 413.0,
               'F19': 1202.0,
               'FRB190520': 1202.0,
               'R190520': 1202.0,
               'FRB190714A': 504.1,
               'FRB210117': 730.0,
               'FRB210320': 384.8,
               'FRB210407': 1785.3,
               'FRB210807': 251.9,
               'FRB211127': 234.83,
               'FRB211212': 206.0,
               'FRB220105': 583.0,
               'FRB20180915A': 371,
               'FRB20181030E': 159,
               'FRB20181226B': 288,
               'R181226': 242,
               'FRB20190103C': 1349,
               'FRB20190118A': 225,
               'FRB20190122C': 690,
               'FRB20190202A': 306,
               'FRB20190518C': 444,
               'FRB20190124C': 302.5,
               'FRB20190111A': 173.1,
               'FRB20181224E': 580.7,
               'FRB20220912A': 220,
               'FRB20190915D': 488.7,
               'FRB20191013D': 523.6,
               'FRB20201114A': 323.2,
               'FRB20200223B': 202.3,
               'FRB190502': 396.8,
               'R220912': 220,
               'R201114': 322.23,
               'NR180915A': 371,
               'NR181030E': 159,
               'NR181226B': 288,
               'NR190103C': 1349,
               'NR190118A': 225,
               'NR190122C': 690,
               'NR190202A': 306,
               'NR190518C': 444,
               'NR190124C': 302.5,
               'NR190111A': 173.1,
               'MR181224E': 580.7,
               'R210117_D': 730,
               'R200223': 202.3,
               'R210117': 730,
               'R220529': 245,
               'R210323C': 289,
               'R200619': 439.8,
               'R200127': 351.3,
               'NR210603A': 500.147,
               'R200929': 414,
               'R191013': 524,
               'R230814A': 696.4,
               'R231001': 364.0,
               '231126A': 201.7,
               'R231126A': 201.7,
               'R240121': 527.3,
               'R240114': 527.7,
               'R240216': 310.0,
               'DIAG_FRB20240114A': 527.7,
               'R240430': 298.4,
               'R230511': 606.0,
               'R191105B': 311.2,
               'XTE1810': 178,
               'R240209A': 176.6,
               'R240209': 176.6,
               'R240619D': 465.0,
               'R240619': 465.0,
               'R240722A': 351.0,
               'R240722': 351.0
    }
    try:
        return FRB_DMs[src]
    except KeyError:
        try:
            cmd = "psrcat -c 'dm' -o short -nohead -nonumber {0}".format(src)
            dm = check_output(cmd, shell=True)
            isPulsar = True
            return float(dm)
        except:
            return None
    except:
        return None


def get_src(fil_file):
    cmd = f'header {fil_file} -source_name'
    src = check_output(cmd, shell=True).decode("utf-8").strip()
    return src

def get_nchan(fil_file):
    cmd = f'header {fil_file} -nchans'
    nchans = check_output(cmd, shell=True).decode("utf-8").strip()
    return int(nchans)

def get_max_freq(fil_file):
    cmd = f'header {fil_file} -fch1'
    fch1 = float(check_output(cmd, shell=True).decode("utf-8").strip())
    cmd = f'header {fil_file} -foff'
    foff = float(check_output(cmd, shell=True).decode("utf-8").strip())
    if foff < 0.0:
        return fch1
    else:
        cmd = f'header {fil_file} -nchans'
        nchans = int(check_output(cmd, shell=True).decode("utf-8").strip())
        return fch1 + nchans*foff

