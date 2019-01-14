import json, subprocess
from threading import Timer, Event, Thread
from subprocess import call, check_output, Popen, PIPE
from decimal import Decimal
import binascii, base64
import requests as req
import datetime, time
from datetime import datetime
import os, sys, socket
import urllib, requests
import pprint
import smtplib, argparse
from email.mime.text import MIMEText
from random import randint
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import Encoders
import os.path as path

# ** Important **
# Do not alter event_log.txt as it keeps record of all active event, results, and payout info

# PARAMS   
wallet_addr ="TRNPhUMr66hcGKVNjhdKUjDuK8SjsqtaAC"   # return change to this wallet
admin_to_address = "patch@techsquad1.io"             # errors sent from this address on fault
admin_from_address = "brian@techsquad1.io"           # errors sent to this address on fault
error_to_address = "bdillon@liv1e.ie"                # errors sent from this address on fault
admin_from_address = "brian@techsquad1.io"           # errors sent to this address on fault
admin_from_pass = "hPWf8Igy"                        # from address pass
admin_provider = "mail.blacknight.com"              # email provider for this address               
odds_api_key = "665aa00a83fc6d662ad5f718161875b0"   # odds api key
tx_fee = 0.001                                      # fee used in creating transactions
payout_liability_threshold = 1000                   # max amount that can be payed out per event before triggering killswitch
bet_liability_threshold = 14000                     # max amount of bets allowed on a bet before being triggering killswitch

# TIME BASED PARAMS
check_events_interval = 20                          # ping events api (in seconds)
check_thread_interval = 60                          # check if threads are active (in seconds)
write_results_interval = 60                         # remove all completed events from event_log.json and put in results_log.txt
post_error_email_interval = 6000
post_fatal_error_email_interval = 60
post_results_email_interval = 43200
events_inactivity_allowance = 43200                 # check for inactivity after interval (in seconds)
any_inactivity_allowance = 180
update_moneyline_after = 1                          # update moneyline odds after (seconds) 
update_spreads_after = 1                            # update spread odds after (seconds) 
update_totals_after = 1                             # update totals odds after (seconds) 43200
before_game_buffer = 600                            # stop updating odds before game start (seconds)
no_results_before_email = 30
stutter_val = 20

# OPCODE PARAMS
version_number = '1'
opcode_sports = '1'
opcode_rounds = '2'
opcode_tname = '3'
opcode_tments = '4'
opcode_prefix = '42'
mapping_tx_type = '01'
event_tx_type = '02'
result_tx_type = '04'
update_moneyline_tx_type = '05'
spread_tx_type = '09'
totals_tx_type = '10'

# OTHER 
oapi_sports_names = ['americanfootball', 'soccer', 'hockey', 'basketball']
oapi_sports_names_UI = ['Football', 'Soccer', 'Hockey', 'Basketball']
oapi_sports_markets = ['spreads', 'totals', 'h2h']
results_after = 30
result_count = 0
current_oapi_index = 2
last_event_post = datetime.now()
last_result_write = datetime.now()
last_error_email = datetime.now()
last_fatal_error_email = datetime.now()
last_results_email = datetime.now()
last_activity = datetime.now()
sent_first_error = False
sent_first_fatal_error = False
sent_first_results_file = True
first_event_posted = False
last_saved_event_id = 0
get_events_thread = None
get_results_thread = None
thread_checker = None
wagerrDir = "/wagerrts/src/wagerr-cli"            
rootDir =  path.abspath(path.join(__file__ ,"../../../.."))
localDir = rootDir + wagerrDir                  
pp = pprint.PrettyPrinter(indent=2)                       

# Class for creating individual threads with timers
class perpetualTimer ():

    def __init__(self,t,hFunction):
       self.t=t
       self.hFunction = hFunction
       self.thread = Timer(self.t,self.handle_function)

    def handle_function(self):
       self.hFunction()
       self.thread = Timer(self.t,self.handle_function)
       self.thread.start()

    def start(self):
       self.thread.start()

    def cancel(self):
       self.thread.cancel()

# Retrieve all event listings and odds from Odds API (6+ API calls)
def getOddsAPIEvents():

    global current_oapi_index, last_activity, before_game_buffer

    errorLiabilityCheck()

    return

    last_activity = datetime.now()
    totalFoundEvts = {}
    totalFoundTments = []

    #current_oapi_index += 1           
    if current_oapi_index == 4:
        current_oapi_index = 0
    
    sport_name = oapi_sports_names_UI[current_oapi_index]
    log('\n{} Fetching events for {} from Odds API'.format(datetime.now(), sport_name))

    available_tments_file = '{}_tments_file.txt'.format(sport_name.lower())
    with open(available_tments_file) as f:
        for line in f:
            totalFoundTments.append(line.replace("\n", ""))
            
    try:
        # Retrieve event data -  Search each tournament for moneyline, spreads and totals markets
        for mkt in oapi_sports_markets:           
            for tment in totalFoundTments:
                parameters = {"sport": tment, "mkt": mkt, "region": 'uk', "apiKey": odds_api_key}
                events_resp = requests.get('https://api.the-odds-api.com/v3/odds?', params=parameters)
                event_data = json.loads(events_resp.content)
                try:
                    event_data = event_data['data']
                except Exception as e:
                    log('Error response from The Odds API: \n{}'.format(events_resp.content))
                    return
                    
                tment = tment.encode('utf-8').strip()

                # Create local object by each event
                for evt in event_data:
                    currEvt = {} 
                    currOdds = {} 
                    h_team = evt['teams'][0].encode('utf-8').strip()           
                    new_id = u' '.join((evt['home_team'][:4], str(evt['commence_time'])[-6:])).encode('utf-8').strip()
                    new_id = new_id.replace(" ", "").lower()
                    now = int(str(time.time()).split('.', 1)[0])
                    currOdds['spreadsPosted'] = False
                    currOdds['totalsPosted'] = False
                    currOdds['moneylinePosted'] = False     
                    currEvt['hasMoneyline'] = False
                    currEvt['hasSpreads'] = False
                    currEvt['hasTotals'] = False 
                    currEvt['killed'] = False 
                    currEvt['sportStr'] = oapi_sports_names_UI[current_oapi_index]
                        
                    # If this event has not been found yet, create a new object
                    if new_id not in totalFoundEvts:         
                        tment_str = tment.split("_",1)[1] 
                        tment_str = tment_str.replace("_", " ").lower()
                        currEvt['tournament'] = tment_str
                        currEvt['sport'] = oapi_sports_names_UI[current_oapi_index]  
                        currEvt['commences'] = str(evt['commence_time'])
                        commenced = True                     
                        commences = float(evt['commence_time'])
                        commences_str = str(evt['commence_time'])
                        now_str = str(time.time())
                        
                        #check if the game has commenced or not
                        commences_str = str(commences_str).split('.')[0]
                        now_str = str(now_str).split('.')[0]

                        # skip if ten minutes before event
                        game_commences = int(commences_str) - before_game_buffer

                        if int(now_str) > game_commences or commences == 0:
                            commenced = True
                            log('Event with internal id {} is over'.format(tment_str))
                        else:
                            commenced = False
                        
                        currEvt['homeTeam'] = h_team
                        
                        # determine name of the away teams
                        if h_team == evt['teams'][1].encode('utf-8').strip():
                            currEvt['awayTeam'] = evt['teams'][0].encode('utf-8').strip()
                        else:
                            currEvt['awayTeam'] = evt['teams'][1].encode('utf-8').strip()
                        
                        currEvt['id'] = new_id
                        currEvt['Odds'] = {}
                        currEvt['Odds']['killed'] = False
                        currOdds = currEvt['Odds']
                    
                    else:
                        currEvt = totalFoundEvts[new_id]
                        currOdds = totalFoundEvts[new_id]['Odds']
                        totalFoundEvts[new_id]['id'] = new_id
                    
                    # If this event has odds for this market, add them to object
                    if len(evt['sites']) > 0 and commenced == False:          
                        
                        # add moneyline data
                        if mkt == 'h2h':
                            moneyline = evt['sites'][0]['odds']['h2h']
                            currOdds['moneyline'] = {}
                            
                            # if draw odds not found set value to 0
                            try:
                                draw_val = evt['sites'][0]['odds']['h2h'][2]
                            except:
                                draw_val = 0

                            newH2HOdds = calibrateOdds(moneyline[0], moneyline[1], draw_val)
                            currOdds['moneyline']['moneylineHome'] = newH2HOdds[0]
                            currOdds['moneyline']['moneylineAway'] = newH2HOdds[1]
                            currOdds['moneyline']['moneylineDraw'] = newH2HOdds[2]
                            currOdds['moneyline']['timeAdded'] = now
                            currEvt['hasMoneyline'] = True
                        
                        # add spread data to local object
                        elif mkt == 'spreads':                          
                            currOdds['spreads'] = {}
                            spreads = evt['sites'][0]['odds']['spreads']
                            newSpreadOdds = calibrateOdds(spreads['odds'][0], spreads['odds'][1], 0)
                            currOdds['spreads']['spreadOver'] = newSpreadOdds[0]
                            currOdds['spreads']['spreadUnder'] = newSpreadOdds[1]
                            
                            # convert to int and check for decimal places    
                            try:
                                spreadOverPoints = str(spreads['points'][0]).replace(".","")
                                spreadUnderPoints = str(spreads['points'][1]).replace(".","")                                    
                            except:
                                spreadOverPoints = int(spreads['points'][0])*10
                                spreadUnderPoints = int(spreads['points'][1])*10
                                                                
                            currOdds['spreads']['spreadOverPoints'] = spreadOverPoints
                            currOdds['spreads']['spreadUnderPoints'] = spreadUnderPoints
                            currOdds['spreads']['timeAdded'] = now 
                            currEvt['hasSpreads'] = True                                
                        
                        # add totals data to local object
                        elif mkt == 'totals':                     
                            currOdds['totals'] = {}
                            totals = evt['sites'][0]['odds']['totals']
                            new_totals = calibrateOdds(totals['odds'][0], totals['odds'][1], 0)
                            currOdds['totals']['hTotalsPosition'] = totals['position'][0]
                            currOdds['totals']['aTotalsPosition'] = totals['position'][1]
                            currOdds['totals']['totalOverPoints'] = totals['points'][0]*10
                            currOdds['totals']['totalUnderPoints'] = totals['points'][1]*10
                            currOdds['totals']['totalOver'] = new_totals[0]
                            currOdds['totals']['totalUnder'] = new_totals[1]
                            currOdds['totals']['timeAdded'] = now
                            currEvt['hasTotals'] = True       
                    
                        totalFoundEvts[new_id] = currEvt
                    
                    else:                        
                        log('No {} odds found for {} in {}'.format(mkt, new_id, tment))  
                        if mkt == 'h2h':
                            try:
                                del totalFoundEvts[new_id] 
                            
                            except:
                                log('Not adding event as no moneyline odds are available')
        
    except Exception as e:
        log('ERROR: Cannot get odds from Odds API {} listings. {}'.format(sport_name, e))       
        return 

    log('Total {} events that are available for betting: {} \nCreating opcodes...'.format(sport_name, len(totalFoundEvts)))
    # Process found events TODO: refactor this section
    for key, evt in totalFoundEvts.items():

        # TODO Needs testing, is this overwriting saved event at this address? need eaher var top start at selected id
        evt['eventid'] = str(last_saved_event_id)

        # Check if event already exists in log, if not, add
        prevEvtData = isAddedToLog(evt, 'event', 'hasEvent')  
        if prevEvtData[0] == False:
            createEventOpCode(evt, prevEvtData[1],prevEvtData[2], prevEvtData[3])

        # TODO Clean up prevEvtData
        # Check if moneyline event already exists in log. Update odds if available  
        prevEvtData = isAddedToLog(evt, 'moneyline', evt['hasMoneyline'])
        if prevEvtData[0] == False:
            createUpdateMoneylineOpCode(evt, prevEvtData[1], prevEvtData[2], prevEvtData[3])

        # Check if spread event already exists in log, if not, add. Update odds if available  
        prevEvtData = isAddedToLog(evt, 'spreads', evt['hasSpreads'])
        if prevEvtData[0] == False:
            createSpreadsOpCode(evt, prevEvtData[1],prevEvtData[2], prevEvtData[3])

        # Check if totals event already exists in log, if not, add. Update odds if available    
        prevEvtData = isAddedToLog(evt, 'totals', evt['hasTotals'])
        if prevEvtData[0] == False:
            createTotalsOpCode(evt, prevEvtData[1],prevEvtData[2], prevEvtData[3])

    # after all events have been posted check for failed transactions and retry
    if len(totalFoundEvts) == 0:
        checkFailedTxQueue()
        errorLiabilityCheck()
                      
# Check if event already exists in log by type. Events are grouped by id.
def isAddedToLog (evt, eventType, hasType):

    dont_update = [True, 0, 0, 0]
    eventid = ''

    if os.path.exists("event_log.json") == False:     
        log('No file exists - Creating new file and appending.')
        c = open("event_log.json","w+")
        c.write('{"Events" : {} }')

    with open("event_log.json", "r") as f:
        rd = f.read()
        if len(rd) == 0 or rd == "{u'Events': {}}":
            return dont_update
        else:
            obj = json.loads(rd)          
            for saved_evt in obj['Events']: 

                try:
                    saved_evt = saved_evt.encode('utf-8').strip()
                    evt['id'] = evt['id'].encode('utf-8').strip()
                except Exception as e:
                    log('Error - cannot parse data from this event - skipping.')
                    return [True, 0, 0, saved_evt]
                    
                # if this event does exist, check if odds need to be updated
                if str(saved_evt) == str(evt['id']):                
                    
                    # only update if new odds are available
                    saved_event = obj['Events'][evt['id']]['Odds']
                    eventid = obj['Events'][evt['id']]['eventid']

                    if eventType != 'event' and hasType == False:
                        return dont_update

                    try:
                        if saved_event['killed'] == True:
                            log('The {} odds for this event been closed as it has reached the bet liability threshold ({})'.format(eventType, eventid))
                            return dont_update

                        if saved_event[eventType]['killed'] == True:
                            log('The {} odds for this event been closed as it has reached the bet liability threshold ({})'.format(eventType, eventid))
                            return dont_update
                    except:
                        pass

                    return checkOddsUpdate(eventType, hasType, saved_event, evt, eventid)

            if eventType == 'event':                      
                return [False, 0, 0, eventid]

    return dont_update

# Check if any active events have passsed the liabilty threshold and need to be closed
def errorLiabilityCheck():

    global bet_liability_threshold, payout_liability_threshold

    log("Checking for any active events that have exceeded the error liability threshold...")
    
    try:
        events_list = subprocess.check_output([localDir, "geteventsliability"])
        events_list = json.loads(events_list)

    except:
        return

    for event in events_list:

        event_id = int(event['event-id']) 
        now = int(str(time.time()).split('.', 1)[0])

        spreads = []
        moneyline = []
        totals = []

        for market in event['markets']:
            if 'moneyline-away-liability' in market:
                moneyline = market
            if 'spreads-over-liability' in market:
                spreads = market
            if 'total-over-liability' in market:
                totals = market

        markets_with_liabilty = checkIsAboveThreshold(moneyline, spreads, totals)

        if markets_with_liabilty > 0 and event_id == 30123:

            #saved_evt = retrieveEventFromLog(event_id, markets_with_liabilty)

            #if saved_evt != 'Already Terminated':
                
            # if event bet count limit has been reached, kill the entire event
            if event['event-bet-count'] >= bet_liability_threshold:
                saved_evt = retrieveEventFromLog(event_id, 'none', 'multi')
                saved_evt['killed'] = True
                saved_evt['Odds']['moneyline']['killed'] = True
                createUpdateMoneylineOpCode(saved_evt, now, 'Kill all moneyline betting', event_id)

                if 'spreads' in saved_evt['Odds']:
                    saved_evt['Odds']['spreads']['killed'] = True
                    createSpreadsOpCode(saved_evt, now, 'Kill all spread betting', event_id)
                
                if 'totals' in saved_evt['Odds']:
                    saved_evt['Odds']['totals']['killed'] = True
                    createTotalsOpCode(saved_evt, now, 'Kill all totals betting', event_id)     

                log("Event ({}) exceeds max betcount threshold, killing. Count: {}.".format(event_id, event['event-bet-count']))
    
            # kill any moneyline events that exceed the payout theshold
            if 'moneyline-home-liability' in moneyline and moneyline['moneyline-home-liability'] >= payout_liability_threshold:
                saved_evt = retrieveEventFromLog(event_id, 'moneyline', 'single')
                if saved_evt != 'Already Terminated':
                    try:
                        saved_evt['Odds']['moneyline']['moneylineHome'] = 0
                        createUpdateMoneylineOpCode(saved_evt, now, 'Kill moneyline home betting', event_id)
                    except:
                        createUpdateMoneylineOpCode(saved_evt, now, 'Terminate moneyline betting', event_id)
            
            if 'moneyline-away-liability' in moneyline and moneyline['moneyline-away-liability'] >= payout_liability_threshold:
                saved_evt = retrieveEventFromLog(event_id, 'moneyline', 'single')
                if saved_evt != 'Already Terminated':
                    try:
                        saved_evt['Odds']['moneyline']['moneylineAway'] = 0
                        createUpdateMoneylineOpCode(saved_evt, now, 'Kill moneyline away betting', event_id)
                    except:
                        createUpdateMoneylineOpCode(saved_evt, now, 'Terminate moneyline betting', event_id)
                
            if 'moneyline-draw-liability' in moneyline and moneyline['moneyline-draw-liability'] >= payout_liability_threshold:
                saved_evt = retrieveEventFromLog(event_id, 'moneyline', 'single')
                if saved_evt != 'Already Terminated':
                    try:
                        saved_evt['Odds']['moneyline']['moneylineDraw'] = 0
                        createUpdateMoneylineOpCode(saved_evt, now, 'Kill moneyline draw betting', event_id)
                    except:
                        createUpdateMoneylineOpCode(saved_evt, now, 'Terminate moneyline betting', event_id)
                    
            # kill any spread events that exceed the payout theshold
            if 'spreads-over-liability' in spreads and spreads['spreads-over-liability'] >= payout_liability_threshold:
                saved_evt = retrieveEventFromLog(event_id, 'spreads', 'single')
                if saved_evt != 'Already Terminated':
                    try:
                        saved_evt['Odds']['spreads']['spreadOver'] = 0
                        createSpreadsOpCode(saved_evt, now, 'Kill spreads over betting', event_id)
                    except:
                        createSpreadsOpCode(saved_evt, now, 'Terminate spreads betting', event_id)
                
            if 'spreads-under-liability' in spreads and spreads['spreads-under-liability'] >= payout_liability_threshold: 
                saved_evt = retrieveEventFromLog(event_id, 'spreads', 'single')
                if saved_evt != 'Already Terminated':
                    try:
                        saved_evt['Odds']['spreads']['spreadUnder'] = 0
                        createSpreadsOpCode(saved_evt, now, 'Kill spreads under betting', event_id)
                    except:
                        createSpreadsOpCode(saved_evt, now, 'Terminate spreads betting', event_id)
                
            if 'spreads-push-liability' in spreads and spreads['spreads-push-liability'] >= payout_liability_threshold:         
                saved_evt = retrieveEventFromLog(event_id, 'spreads', 'multi')
                if saved_evt != 'Already Terminated':
                    createSpreadsOpCode(saved_evt, now, 'Terminate spreads betting', event_id)

            # kill any totals events that exceed the payout theshold   
            if 'total-over-liability' in totals and totals['total-over-liability'] >= payout_liability_threshold:
                saved_evt = retrieveEventFromLog(event_id, 'totals', 'single')
                if saved_evt != 'Already Terminated':
                    try:
                        saved_evt['Odds']['totals']['totalsOver'] = 0
                        createTotalsOpCode(saved_evt, now, 'Kill totals over betting', event_id)
                    except:
                        createTotalsOpCode(saved_evt, now, 'Terminate totals', event_id)      
            
            if 'total-under-liability' in totals and totals['total-under-liability'] >= payout_liability_threshold:
                saved_evt = retrieveEventFromLog(event_id, 'totals', 'single')
                if saved_evt != 'Already Terminated':
                    try:
                        saved_evt['Odds']['totals']['totalsUnder'] = 0
                        createTotalsOpCode(saved_evt, now, 'Kill totals under betting', event_id)
                    except:
                        createTotalsOpCode(saved_evt, now, 'Terminate totals betting', event_id)
                
            if 'total-push-liability' in totals and totals['total-push-liability'] >= payout_liability_threshold:
                saved_evt = retrieveEventFromLog(event_id, 'totals', 'multi')
                if saved_evt != 'Already Terminated':
                    createTotalsOpCode(saved_evt, now, 'Terminate totals betting', event_id)

def checkIsAboveThreshold(moneyline, spreads, totals): 

    global payout_liability_threshold

    markets_with_liabilty = []
    
    try: 
        for x in moneyline.items():
            if x[1] >= payout_liability_threshold: 
                markets_with_liabilty.append('moneyline')
    except:
        #log('no moneyline for this event')
        pass

    try:
        for y in spreads.items(): 
            if y[1] >= payout_liability_threshold: 
                markets_with_liabilty.append('spreads')
    except:
        #log('no spreads for this event')
        pass

    try:
        for z in totals.items(): 
            if z[1] >= payout_liability_threshold: 
                markets_with_liabilty.append('totals')
    except:
        #log('no totals for this event')
        pass
    # TODO: Check values are correct, are skipped correctly 

    return markets_with_liabilty

# Pul event object from events_log.json
def retrieveEventFromLog(evtid, market, killType):
    
    with open("event_log.json", "r") as f:
        rd = f.read()
        if len(rd) == 0 or rd == "{u'Events': {}}":
            return 0
        else:
            obj = json.loads(rd) 
          
            if killType == 'multi':
                try:
                    if evtid in obj['Terminated'][market]:
                        log('Event id {} ({}) has already been terminated'.format(evtid, market))
                        return 'Already Terminated'
                except:
                    log('\nEvent id {} ({}) has not been terminated yet'.format(evtid, market))
                    pass

            for key, event in obj['Events'].iteritems():     
 
                try:
                    if str(event['eventid']) == str(evtid): 
                        log('Found event id {} from logs'.format(evtid))

                        if event['killed'] == True:
                            log('Event id {} has been terminated'.format(evtid))
                            return 'Already Terminated'
                
                        return event
                except:
                    pass

            #log('\nCannot find local id to event id ({}) over threshold, killing entire event (all odds set to 0)'.format(evtid))
            event = {'event-id' : evtid, 'id' : ' No local reference' , 'killed':True, 'Odds': { } }
            
            return event

# Check for any changes in odds from the API since the last time it was pinged, update
def checkOddsUpdate(eventType, hasType, savedEvent, newEvt, evtid):

    # stutter updating each events odds by seconds
    stutter_posting = randint(0, stutter_val)
    dont_update = [True, 0, 0, 0]
    now = int(str(time.time()).split('.', 1)[0])
 
    # update moneyline event if new odds found. Otherwise update after specified interval
    if eventType == 'moneyline' and hasType == True:
        try:
            try:
                moneyline_added = savedEvent['moneyline']['timeAdded']
            except:
                moneyline_added = now
            moneyline_posted = savedEvent['moneylinePosted']   
            updateInterval = update_moneyline_after + stutter_posting               
            since = datetime.now() - datetime.utcfromtimestamp(moneyline_added)

            all_same = []
            
            # strictly compare individual moneyline odds for changes 
            if newEvt['Odds']['moneyline']['moneylineHome'] == savedEvent['moneyline']['moneylineHome']:
                all_same.append(True)
            elif savedEvent['moneyline']['moneylineHome'] == 0:
                log('MoneyLine Home betting for event id {} have been killed. Odds update disallowed.'.format(evtid))
                all_same.append(True)
            else:
                all_same.append(False)

            if newEvt['Odds']['moneyline']['moneylineAway'] == savedEvent['moneyline']['moneylineAway']:
                all_same.append(True)
            elif savedEvent['moneyline']['moneylineAway'] == 0:
                log('MoneyLine Away betting for event id {} have been killed. Odds update disallowed.'.format(evtid))
                all_same.append(True)
            else:
                all_same.append(False)

            if newEvt['Odds']['moneyline']['moneylineDraw'] == savedEvent['moneyline']['moneylineDraw']:
                all_same.append(True)
            elif savedEvent['moneyline']['moneylineDraw'] == 0:
                log('MoneyLine Draw betting for event id {} have been killed. Odds update disallowed.'.format(evtid))
                all_same.append(True)
            else:
                all_same.append(False)
            
            if False in all_same:
                if moneyline_posted == False or since.total_seconds() > updateInterval:
                    return [False, moneyline_added, moneyline_posted, evtid]
                else:
                    return dont_update
            else:
                return dont_update

        except Exception as e:
            pass

    # if no spreads event is found for this id, create one. Otherwise update after period
    elif eventType == 'spreads' and hasType == True:
        
        try:
            try:
                spreads_added = savedEvent['spreads']['timeAdded']  
            except:
                spreads_added = now
            since = datetime.now() - datetime.utcfromtimestamp(spreads_added) 
            updateInterval = update_spreads_after + stutter_posting
            spreads_posted = savedEvent['spreadsPosted']
            all_same = []

            # strictly compare individual spread odds for any changes 
            if newEvt['Odds']['spreads']['spreadOver'] == savedEvent['spreads']['spreadOver']:
                all_same.append(True)
            elif savedEvent['spreads']['spreadOver'] == 0:
                log('Spread (Over) betting for event id {} have been killed. Odds update disallowed.'.format(evtid))
                all_same.append(True)
            else:
                all_same.append(False)

            if newEvt['Odds']['spreads']['spreadUnder'] == savedEvent['spreads']['spreadUnder']:
                all_same.append(True)
            elif savedEvent['spreads']['spreadUnder'] == 0:
                log('Spread (Under) betting for event id {} have been killed. Odds update disallowed.'.format(evtid))
                all_same.append(True)
            else:
                all_same.append(False)

            if newEvt['Odds']['spreads']['spreadOverPoints'] == savedEvent['spreads']['spreadOverPoints']:
                all_same.append(True)
            elif savedEvent['spreads']['spreadOverPoints'] == 0:
                log('Spread (Home) betting for event id {} have been killed. Points update disallowed.'.format(evtid))
                all_same.append(True)
            else:
                all_same.append(False)

            if newEvt['Odds']['spreads']['spreadUnderPoints'] == savedEvent['spreads']['spreadUnderPoints']:
                all_same.append(True)
            elif savedEvent['spreads']['spreadUnderPoints'] == 0:
                log('Spread (Away) betting for event id {} have been killed. Points update disallowed.'.format(evtid))
                all_same.append(True)
            else:
                all_same.append(False)

            if False in all_same:
                if spreads_posted == False or since.total_seconds() > updateInterval:
                    return [False, spreads_added, spreads_posted, evtid]
                else:
                    return dont_update
            else:
                return dont_update

        except Exception as e:
            pass

    # if no totals event is found for this id, create one. Otherwise update after specified period
    elif eventType == 'totals' and hasType == True:
        try:
            try:
                totals_added = savedEvent['totals']['timeAdded']
            except:
                totals_added = now
            totals_posted = savedEvent['totalsPosted']
            updateInterval = update_totals_after + stutter_posting
            since = datetime.now() - datetime.utcfromtimestamp(totals_added) 
            all_same = []

            # strictly compare individual moneyline odds for differences 
            if newEvt['Odds']['totals']['totalOverPoints'] == savedEvent['totals']['totalOverPoints']:
                all_same.append(True)
            elif savedEvent['totals']['totalOverPoints'] == 0:
                log('Totals (Home) betting for event id {} have been killed. Points update disallowed.'.format(evtid))
                all_same.append(True)
            else:
                all_same.append(False)
            
            if newEvt['Odds']['totals']['totalUnderPoints'] == savedEvent['totals']['totalUnderPoints']:
                all_same.append(True)
            elif savedEvent['totals']['totalUnderPoints'] == 0:
                log('Totals (Away) betting for event id {} have been killed. Points update disallowed.'.format(evtid))
                all_same.append(True)
            else:
                all_same.append(False) 
        
            if newEvt['Odds']['totals']['totalOver'] == savedEvent['totals']['totalOver']:
                all_same.append(True)
            elif savedEvent['totals']['totalOver'] == 0:
                log('Totals (Home) betting for event id {} have been killed. Odds update disallowed.'.format(evtid))
                all_same.append(True)
            else:
                all_same.append(False)

            if newEvt['Odds']['totals']['totalUnder'] == savedEvent['totals']['totalUnder']:
                all_same.append(True)
            elif savedEvent['totals']['totalUnder'] == 0:
                log('Totals (Away) betting for event id {} have been killed. Odds update disallowed.'.format(evtid))
                all_same.append(True)
            else:
                all_same.append(False)

            if False in all_same:
                if totals_posted == False or since.total_seconds() > updateInterval:
                    return [False, totals_added, totals_posted, evtid]
                else:
                    return dont_update
            else:
                return dont_update

        except Exception as e:
            pass

    else:
        return dont_update

    return dont_update

# Create events opCode and add new event to blockchain
def createEventOpCode (evt, added, posted, evtid):

    # Retrieve current event object values
    event_tx_type_hx = '{:02x}'.format(int(event_tx_type))
    version_number_hx = '{:02x}'.format(int(version_number))
    id_hx = '{:02x}'.format(int(evt['eventid']))
    timestamp_hx = '{:02x}'.format(int(evt['commences']))
    home_odds_hx = '{:02x}'.format(int(evt['Odds']['moneyline']['moneylineHome']))
    away_odds_hx = '{:02x}'.format(int(evt['Odds']['moneyline']['moneylineAway']))
    draw_odds_hx = '{:02x}'.format(int(evt['Odds']['moneyline']['moneylineDraw']))

    try:
        # Retrieve mappings from RPC
        sport_mapping = subprocess.check_output([localDir, "getmappingid", "sports", str(evt['sportStr'])])
        tournament_mapping = subprocess.check_output([localDir, "getmappingid", "tournaments", str(evt['tournament'])])
        team_mappingA = subprocess.check_output([localDir, "getmappingid", "teamnames", str(evt['homeTeam'])])
        team_mappingB = subprocess.check_output([localDir, "getmappingid", "teamnames", str(evt['awayTeam'])])
    
    except Exception as e:
        log('Error: no response from getmappings - cannot connect to server')
        return

    # TODO: Round mappings are currently ignored. value is hardcoded as string in opcode
    sportRes = json.loads(sport_mapping)[0]
    tournamentRes = json.loads(tournament_mapping)[0]
    teamARes = json.loads(team_mappingA)[0]
    teamBRes = json.loads(team_mappingB)[0]

    # Convert mappings from RPC to hex decimal string
    sport_hx = '{:02x}'.format(int(sportRes['mapping-id']))
    tment_hx = '{:02x}'.format(int(tournamentRes['mapping-id']))
    home_team_hx = '{:02x}'.format(int(teamARes['mapping-id']))
    away_team_hx = '{:02x}'.format(int(teamBRes['mapping-id']))

    sportRes['newVal'] = str(evt['sportStr'])
    tournamentRes['newVal'] = str(evt['tournament'])
    teamARes['newVal'] = str(evt['homeTeam'])
    teamBRes['newVal'] = str(evt['awayTeam'])

    # Create and post new mapping transactions where neccesary
    createMappingOpcodes(sportRes, tournamentRes, 0, teamARes, teamBRes, version_number_hx, evt)

    # Pad value with zeroes to satisfy opcode requirements
    id_hx = str(id_hx).zfill(8) 
    sport_hx = str(sport_hx).zfill(4)
    tment_hx = str(tment_hx).zfill(4)
    home_team_hx = str(home_team_hx).zfill(8) 
    away_team_hx = str(away_team_hx).zfill(8) 
    home_odds_hx = str(home_odds_hx).zfill(8) 
    away_odds_hx = str(away_odds_hx).zfill(8) 
    draw_odds_hx = str(draw_odds_hx).zfill(8) 
    timestamp_hx = str(timestamp_hx).zfill(8) 

    # Create opcode from event data
    op_code = ""+opcode_prefix+""+version_number_hx+""+event_tx_type_hx+""+id_hx+""+timestamp_hx+""+sport_hx+""+tment_hx+"0000"+home_team_hx+""+away_team_hx+""+home_odds_hx+""+away_odds_hx+""+draw_odds_hx+""
    log ("\n*** ({}/{}) Creating event - {} vs {} ***\nopcode: {} ".format(evt['eventid'], evt['id'], evt['homeTeam'], evt['awayTeam'],op_code))  
    postOpCode(op_code, evt, 'Event')
    
    return

# If a mapping has not been found for event data, create new mapping opcode
def createMappingOpcodes (sportMap, tmentMap, stageMap, teamAMap, teamBMap, versionHX , evt):

    # Create new mapping for sport
    if sportMap['exists'] == False:        
        paddedNamespace = str(opcode_sports).zfill(2)
        hex_id = '{:02x}'.format(int(sportMap['mapping-id']))
        padded_id = str(hex_id).zfill(4)
        new_val = binascii.hexlify(sportMap['newVal'])
        map_op_code = ""+opcode_prefix+""+versionHX+""+mapping_tx_type+""+paddedNamespace+""+padded_id+""+new_val+""
        log('\n*** Creating sports mapping "{}" with index {} *** '.format(str(evt['sportStr']), padded_id))
        log('Opcode {} ***'.format(map_op_code)) 
        postOpCode(map_op_code, 'Mapping', 0)

    # Create new mapping for tournament
    if tmentMap['exists'] == False:        
        new_val = binascii.hexlify(tmentMap['newVal'])
        paddedNamespace = str(opcode_tments).zfill(2)
        hex_id = '{:02x}'.format(int(tmentMap['mapping-id']))
        padded_id = str(hex_id).zfill(4)
        map_op_code = ""+opcode_prefix+""+versionHX+""+mapping_tx_type+""+paddedNamespace+""+padded_id+""+new_val+""      
        log('\n*** Creating tournament mapping "{}" with index {} ***'.format(str(evt['tournament']), padded_id))
        log('Opcode {} ***'.format(map_op_code)) 
        postOpCode(map_op_code, 'Mapping', 0)
          
    # Create new mapping for team A
    if teamAMap['exists'] == False:      
        new_val = binascii.hexlify(teamAMap['newVal'])
        paddedNamespace = str(opcode_tname).zfill(2)
        hex_id = '{:02x}'.format(int(teamAMap['mapping-id']))
        padded_id = str(hex_id).zfill(8)
        map_op_code = ""+opcode_prefix+""+versionHX+""+mapping_tx_type+""+paddedNamespace+""+padded_id+""+new_val+""     
        log('\n*** Creating team mapping "{}" with index {} ***'.format(str(evt['homeTeam']), padded_id)) 
        log('Opcode {} ***'.format(map_op_code)) 
        postOpCode(map_op_code, 'Mapping', 0)

    # Create new mapping for team B
    if teamBMap['exists'] == False:      
        new_val = binascii.hexlify(teamBMap['newVal'])
        paddedNamespace = str(opcode_tname).zfill(2)
        hex_id = '{:02x}'.format(int(teamBMap['mapping-id']))
        padded_id = str(hex_id).zfill(8)
        map_op_code = ""+opcode_prefix+""+versionHX+""+mapping_tx_type+""+paddedNamespace+""+padded_id+""+new_val+""     
        log('\n*** Creating team mapping "{}" with index {} ***'.format(str(evt['awayTeam']), padded_id)) 
        log('Opcode {} ***'.format(map_op_code)) 
        postOpCode(map_op_code, 'Mapping', 0)

# Create opCode for Totals event and add to blockchain
def createTotalsOpCode (evt, addedOn, evtType, evtid):
    
    home_totals_position = ''
    
    if 'Terminate' in evtType:
        # evtOdds['totalOver'] = 0
        # evtOdds['totalUnder'] = 0
        # evtOdds['totalOverPoints'] = 0
        # evtOdds['totalUnderPoints'] = 0
        home_totals_points = 0
        away_totals_points = 0
        home_totals_odds = 0
        away_totals_odds = 0
        home_totals_position = 'over'
    else:
        evtOdds = evt['Odds']['totals']
        home_totals_position = evtOdds['hTotalsPosition']
        home_totals_points = int(evtOdds['totalOverPoints'])
        away_totals_points = int(evtOdds['totalUnderPoints'])
        home_totals_odds = int(evtOdds['totalOver'])
        away_totals_odds = int(evtOdds['totalUnder'])   

    # determine which team has over odds
    if home_totals_position == 'over':
        over_odds = home_totals_odds
        under_odds = away_totals_odds
    else:
        over_odds = away_totals_odds
        under_odds = home_totals_odds
    
    # Convert opcode members to hex representation.
    evt_id_hx = '{:02x}'.format(int(evtid))  
    totals_tx_type_hx = '{:02x}'.format(int(totals_tx_type))
    version_number_hx = '{:02x}'.format(int(version_number))
    home_totals_points_hx = '{:02x}'.format(home_totals_points)
    away_total_points_hx = '{:02x}'.format(away_totals_points)
    under_odds_hx = '{:02x}'.format(under_odds)
    over_odds_hx = '{:02x}'.format(over_odds)

    # Create padding where neccesary
    evt_id_hx = str(evt_id_hx).zfill(8)
    home_totals_points_hx = str(home_totals_points_hx).zfill(4)
    away_total_points_hx = str(away_total_points_hx).zfill(4)
    under_odds_hx = str(under_odds_hx).zfill(8)
    over_odds_hx = str(over_odds_hx).zfill(8)
    
    # add to debug log
    if evtType == True:
        time_added = '. Last time added: {}'.format(datetime.utcfromtimestamp(addedOn))
        action_type = 'Updating Totals event'
        save_as_type = 'Totals'
    elif 'Terminate' in evtType:
        action_type = evtType
        save_as_type = 'Terminate totals event'
        time_added = ''
    elif 'Kill' in evtType:
        action_type = evtType
        save_as_type = 'Kill Totals'
        time_added = ''
    else:
        action_type = 'Creating new totals event'
        save_as_type = 'Totals'
        time_added = ''

    # Create OP CODE from totals odds data
    op_code = "" + opcode_prefix + version_number_hx + totals_tx_type_hx + evt_id_hx + home_totals_points_hx + over_odds_hx + under_odds_hx+""
    log ("\n*** ({}/{}) {} - Home points: {} Over odds: {}, Under odds: {}{} ***\nOpcode: {} ".format(int(evtid), evt['id'], action_type, home_totals_points, over_odds, under_odds, time_added, op_code))
    postOpCode(op_code, evt, save_as_type)

    return

# Create results opCode and add new result event to blockchain
def createSpreadsOpCode (evt, addedOn, evtType, evtid):

    if 'Terminate' in evtType:
        h_spread_points = 0
        a_spread_points = 0
        h_spread_odds = 0
        a_spread_odds = 0
    else:
        # TODO Ensure no consequences to removing odds
        h_spread_points = int(evt['Odds']['spreads']['spreadOverPoints'])
        a_spread_points = int(evt['Odds']['spreads']['spreadUnderPoints'])
        h_spread_odds = int(evt['Odds']['spreads']['spreadOver'])
        a_spread_odds = int(evt['Odds']['spreads']['spreadUnder'])

    # convert to absolute number
    h_points = abs(h_spread_points)
    a_points = abs(a_spread_points)
    h_odds = abs(h_spread_odds)
    a_odds = abs(a_spread_odds)
    
    # convert opcode members to hex representation
    evt_id_hx = '{:02x}'.format(int(evtid)) 
    spread_tx_type_hx = '{:02x}'.format(int(spread_tx_type))
    version_number_hx = '{:02x}'.format(int(version_number))
    h_spread_points_hx = '{:02x}'.format(h_points)
    a_spread_points_hx = '{:02x}'.format(a_points)
    h_spread_odds_hx = '{:02x}'.format(h_odds)
    a_spread_odds_hx = '{:02x}'.format(a_odds)

    # create padding
    evt_id_hx = str(evt_id_hx).zfill(8) 
    h_spread_points_hx = str(h_spread_points_hx).zfill(4)
    a_spread_points_hx = str(a_spread_points_hx).zfill(4)
    h_spread_odds_hx = str(h_spread_odds_hx).zfill(8)
    a_spread_odds_hx = str(a_spread_odds_hx).zfill(8)

    # TODO Refactor eventTyoe
    # add to debug log
    save_as_type = 'Spreads'
    
    if evtType == True:
        time_added = '. Last time added: {}'.format(datetime.utcfromtimestamp(addedOn))
        action_type = 'Updating Spreads event'
    elif 'Terminate' in evtType:
        action_type = evtType
        time_added = ''
        save_as_type = 'Terminate spreads'
    elif 'Kill' in evtType:
        action_type = evtType
        time_added = ''
    else:
        action_type = 'Creating new Spreads event'
        time_added = ''

    # create OP CODE from data
    op_code = "" + opcode_prefix + version_number_hx + spread_tx_type_hx + evt_id_hx + h_spread_points_hx + h_spread_odds_hx + a_spread_odds_hx + ""
    log ("\n*** ({}/{}) {} - Home pts: {}, Away pts: {}, Home odds: {}, Away odds: {}{}  ***\nOpcode: {}".format(int(evtid), evt['id'], action_type, h_spread_points, a_spread_points, h_spread_odds, a_spread_odds, time_added, op_code))
    postOpCode(op_code, evt, save_as_type)

    return

# Create opCode for new moneyline odds and add to blockchain
def createUpdateMoneylineOpCode(evt, addedOn, evtType, evtid):
    
    if 'Terminate' in evtType:
        home_moneyline = 0
        away_moneyline = 0
        draw_moneyline = 0
    else:
        home_moneyline = evt['Odds']['moneyline']['moneylineHome']
        away_moneyline = evt['Odds']['moneyline']['moneylineAway']
        draw_moneyline = evt['Odds']['moneyline']['moneylineDraw']
    
    # convert opcode members to hex representation.
    update_moneyline_tx_type_hx = '{:02x}'.format(int(update_moneyline_tx_type))
    version_number_hx = '{:02x}'.format(int(version_number))
    evt_id_hx = '{:02x}'.format(int(evtid))
    home_ml_hx = '{:02x}'.format(int(home_moneyline))
    away_ml_hx = '{:02x}'.format(int(away_moneyline))
    draw_ml_hx = '{:02x}'.format(int(draw_moneyline))

    # create padding
    home_ml_hx = str(home_ml_hx).zfill(8) 
    away_ml_hx = str(away_ml_hx).zfill(8) 
    draw_ml_hx = str(draw_ml_hx).zfill(8) 
    evt_id_hx = str(evt_id_hx).zfill(8)
  
    timeAddedStr = 'Last time added: {}'.format(datetime.utcfromtimestamp(addedOn))
    
    save_as_type = 'Moneyline'
    if evtType == True:       
        action_type = 'Updating Moneyline odds'
        timeAddedStr = '. Added: {}'.format(datetime.utcfromtimestamp(addedOn))
    elif evtType == False:
        action_type = 'Creating update moneyline odds event'
        timeAddedStr = '. Creating on: {}'.format(datetime.utcfromtimestamp(addedOn)) 
    if 'Terminate' in evtType:
        action_type = evtType
        timeAddedStr = 'Killed on: {}'.format(datetime.utcfromtimestamp(addedOn))
        save_as_type = 'Terminate moneyline'
    else:
        action_type = evtType
        timeAddedStr = 'Killed on: {}'.format(datetime.utcfromtimestamp(addedOn))
        
    # create OP CODE from data
    op_code = "" + opcode_prefix + version_number_hx + update_moneyline_tx_type_hx + evt_id_hx + home_ml_hx + away_ml_hx + draw_ml_hx + ""
    log ("\n*** ({}/{}) {} . home: {}, away: {}, draw: {}. {} \nOpcode: {} ***".format(int(evtid), evt['id'], action_type, home_moneyline, away_moneyline, draw_moneyline, timeAddedStr, op_code))
    postOpCode(op_code, evt, save_as_type)

    return

# Create and send a transaction from the opcode
def postOpCode (op_code, evt, evtId):
    
    try: 
        # List unspent outputs
        utxo_list = subprocess.check_output([localDir, "listunspent"])
        utxo_list_json = json.loads(utxo_list)   
    
    except Exception as e:       
        log('Error: Server cant connect. Try restarting the wagerr daemon or checking your internet connection.')          
        
        if evt == 'Mapping':          
            saveToFile(evt, 'addToQueue', op_code)
            log('Error posting mapping transaction, added to queue')         
        return
    try:
        # Check for sufficient funds in UT
        requiredTxList = json.dumps(utxo_list_json[0])
        requiredTxList = '['+requiredTxList+']'

        # List unspent outputs
        formatted_utxo_list = getRequiredTxList(utxo_list, tx_fee)

        # Calculate change
        spend = formatted_utxo_list['amt']
        change = str(float(spend) - float(tx_fee))
        log('Total spend is {}. Total UTXO change is {}.'.format(spend, change))

        # Create the transaction JSON.
        transactionArray = "{\"" + wallet_addr + "\":" + change + ",\"""data\":\"" + op_code + "\"}"
        #log ("Transaction: {} ".format(transactionArray))

        # Create raw transaction
        raw_tx = subprocess.check_output([localDir, "createrawtransaction", formatted_utxo_list['txList'], transactionArray]).rstrip()
        raw_tx = raw_tx.decode('utf-8')

        # Sign the TX
        signed_tx = subprocess.check_output([localDir, "signrawtransaction", raw_tx])
        signed_tx = signed_tx.decode('utf-8')
        signed_tx_json = json.loads(signed_tx)
        formatted_signed_tx = signed_tx_json['hex']

    except Exception as e:
        log('ERROR cannot find any unspent UTXOs, terminating: {}'.format(e))
        
        if evt == 'Mapping':
            saveToFile(evt, 'addToQueue', op_code)
            log('Error posting mapping transaction, added to queue')         
        return
     
    # Send the signed TX and update events log where necessary 
    #result = subprocess.check_output([localDir, "sendrawtransaction", formatted_signed_tx]).rstrip()       
    result = ''

    if 'error' in result: 
        log ('Could not complete transaction: {}'.format(result))
        return      
    else:
        if evtId == 'Event':            
            # On success, save event to file and check queue for failed transactions
            event_ID = int(evt['eventid'])
            saveToFile(evt, 'Event', event_ID)
            log('Event posted to chain (internal id: {}) at {}\nResult {}:'.format(event_ID, datetime.now(), result))
            checkFailedTxQueue()
            errorLiabilityCheck()
        
        elif evt == 'Mapping':
            # On success, remove mapping from the queue 
            log('Mapping posted to chain at {} \nResult {}:'.format(datetime.now(), result))
            saveToFile(evt, 'removeFromQueue', op_code)
        
        elif evtId == 'Moneyline':
            # On success, mark moneyline event as posted
            log('Moneyline update posted to chain at {} \nResult {}:'.format(datetime.now(), result))
            saveToFile(evt, 'Moneyline', op_code)  
        
        elif evtId == 'Spreads':
            # On success, mark spread event as posted
            log('Spreads event posted to chain at {} \nResult {}:'.format(datetime.now(), result))
            saveToFile(evt, 'Spreads', op_code)  
        
        elif evtId == 'Totals':
            # On success, mark totals event as posted
            log('Totals event posted to chain at {} \nResult {}:'.format(datetime.now(), result))
            saveToFile(evt, 'Totals', op_code)
        
        elif 'Terminate' in evtId:
            # On success, mark totals event as posted
            log('Event terminated and posted to chain at {} \nResult {}:'.format(datetime.now(), result))
            saveToFile(evt, evtId, op_code) 

        # TODO rename evtid 
        elif 'Kill' in evtId:
            # On success, mark totals event as posted
            log('{} event posted to chain at {} \nResult {}:'.format(evtId, datetime.now(), result))
            saveToFile(evt, 'Kill', op_code) 

        else: 
            # On success, remove event from file and mark as successful in event log file 
            event_ID = int(evt['eventid'])
            log('{} - Result posted to chain (internal id:{}), This event will be removed from local file \nResult {}:'.format(datetime.now(), event_ID, result))
            checkFailedTxQueue()
            errorLiabilityCheck()
            saveToFile(evt, 'Result', evtId)           

# Retry posting failed transactions that did not succeed
def checkFailedTxQueue ():

    if os.path.exists("event_log.json"):
        with open("event_log.json", "r") as f:

            obj = json.loads(f.read())
            
            if 'Queue' in obj:
                failedTXQueue = obj['Queue']
                failedTXQueue = list(set(failedTXQueue))

                for i in failedTXQueue:
                    log('\n *** Found opcode waiting in queue: {} ***'.format(i)) 
                    failedTXQueue.remove(i)     
                    postOpCode(i, 'Mapping' , 0)

                obj['Queue'] = failedTXQueue
                dump = json.dumps(obj)
                
                with open("event_log.json", "w") as d:
                    d.write(dump) 

# Find the last known event ID
def getCurrentGame ():
    
    log('Finding most recent event...')
    most_recent_event = 0

    # Create event log if it doesnt already exist
    if os.path.exists("event_log.json") == False:
        log('Creating new file and appending.')
        new_file = open("event_log.json","w+")
        obj = {}
        obj['id'] = 0
        obj['commences'] = 0
        dump = json.dumps(obj)
        new_file.write('{ "Events" : '+dump+'}')

    # Find in file
    with open("event_log.json", "r") as f:
        rd = f.read()
        if len(rd) == 0:
            log('No events added yet.')
        else:
            obj = json.loads(rd)
            if 'LastEventID' in obj:
                most_recent_event = obj['LastEventID']              

    log('Most recent event ID is {}'.format(most_recent_event))

    return most_recent_event

# Process and save events to local JSON file
def saveToFile (evt, evtType, evtId):

    global last_event_post, last_saved_event_id, first_event_posted

    # if no event log exists, add
    if os.path.exists("event_log.json"):
        with open("event_log.json", "r") as f:
            rd = f.read()

            # If nothing in file, add some placeholder JSON 
            if len(rd) == 0:                                   
                log('Saving, nothing in file')
                
                with open("event_log.json", "w") as c:
                    obj = {}
                    obj['Events'][str(evt['id'])] = evt
                    dump = json.dumps(obj)
                    c.write('{"Events" : {} }'.format())       
            else:               
                obj = json.loads(rd)

                # Add new event to events log 
                if evtType == 'Event':                     
                    log('Saving new event to file with id {}'.format(last_saved_event_id))                
                    obj['Events'][evt['id']] = evt
                    obj['Events'][evt['id']]['Odds']['moneylinePosted'] = True
                    obj['Events'][evt['id']]['Odds']['totalsPosted'] = False
                    obj['Events'][evt['id']]['Odds']['spreadsPosted'] = False             
                    
                    # move to next id number
                    last_saved_event_id += 1
                    obj['LastEventID'] = last_saved_event_id
                    
                    writeToFile(obj)
                    last_event_post = datetime.now()
                    first_event_posted = True
                
                # Add mapping transaction opcode to queue      
                elif evtType == 'addToQueue':                                                                  
                    log('Adding mapping tx opcode to queue: {}'.format(evtId))
                    
                    if 'Queue' in obj:
                        obj['Queue'].append(evtId)
                    else:
                        obj['Queue'] = [evtId]
                              
                    writeToFile(obj)
                
                # Remove mapping transaction opcode from queue
                elif evtType == 'removeFromQueue':
                    log('Removing map transaction from queue: {}'.format(evtId))           

                    if 'Queue' in obj:
                        if evtId in obj['Queue']: 
                            obj['Queue'].remove(evtId)

                    writeToFile(obj)

                # Mark moneyline event as posted
                elif evtType == 'Moneyline':                                                                                        
                    obj['Events'][evt['id']]['Odds']['moneylinePosted'] = True
                    obj['Events'][evt['id']]['Odds']['moneyline'] = evt['Odds']['moneyline']                    
                    writeToFile(obj)
                    last_event_post = datetime.now()

                # Mark spread event as posted 
                elif evtType == 'Spreads':                                                                                    
                    obj['Events'][evt['id']]['Odds']['spreadsPosted'] = True
                    obj['Events'][evt['id']]['Odds']['spreads'] = evt['Odds']['spreads']
                    writeToFile(obj)
                    last_event_post = datetime.now()

                # Mark totals event as posted 
                elif evtType == 'Totals':                                                                                      
                    obj['Events'][evt['id']]['Odds']['totalsPosted'] = True
                    obj['Events'][evt['id']]['Odds']['totals'] = evt['Odds']['totals']
                    writeToFile(obj)
                    last_event_post = datetime.now()

                #elif evtType == 'Kill Event':                                                                                        
                #    obj['Events'][evt['id']]['Odds']['killed'] = True               
                #    writeToFile(obj)
                
                # Set all moneyline odds to 0
                elif 'Terminate' in evtType:     
                    #log('*** SUCCESSFULLY KILLED UNREFED EVENT {} - saving {} event ***'.format(evt['id'], evtType)) 
                    #print(obj)
                    #print(evtType)

                    if 'moneyline' in evtType:
                        market = 'moneyline'
                    if 'spreads' in evtType:
                        market = 'spreads'
                    if 'totals' in evtType:
                        market = 'totals'                                                                                
                    
                    if 'Terminated' in obj and obj['Terminated'] != None:

                        terminatedArray = obj['Terminated']
                        print(market)

                        if market in terminatedArray:
                            market_items = terminatedArray[market]
                            if evt['event-id'] not in market_items:
                                market_items.append(evt['event-id'])
                                terminatedArray[market] = market_items
                                log(terminatedArray)  
                                obj['Terminated'] = terminatedArray
                                writeToFile(obj)
                            else:
                                log('not saving id to terminated array as it already exists')
                        else:
                            market_items = []
                            market_items.append(evt['event-id'])
                            terminatedArray[market] = market_items
                            
                    else:
                        terminatedArray = {}
                        market_items = []
                        market_items.append(evt['event-id'])
                        terminatedArray[market] = market_items
                        log(terminatedArray)  
                        obj['Terminated'] = terminatedArray
                        writeToFile(obj)
                              
    else:
        log('Creating new file and appending.')
        new_file = open("event_log.json","w+")
        try: 
            obj['Events'][evt['id']] = evt
            dump = json.dumps(obj)
            new_file.write('{ "Events" : '+dump+'}')
        except:
            log('ERROR saving to file {}'.format(evt))

# Write the object to the file
def writeToFile(obj):

    dump = json.dumps(obj)
    with open("event_log.json", "w") as d:
        d.write(dump)

# Remove event from event_log.txt and add to results_log.txt
def writeToResultsFile():

    global last_result_write, no_results_before_email, result_count

    log("Checking for outdated events...")
    last_result_write = datetime.now()
    testarray = [0,'commences', 'moneylinePosted', 'spreadsPosted','totalsPosted','id']
    to_remove = []

    if os.path.exists("event_log.json") == False:
        log('No event_log file found to read from')
        return

    if os.path.exists("results_log.txt") == False:
        print('No file exists - Creating new result file and appending at {}.'.format(datetime.now()))
        c = open("results_log.txt","w+")
        c.write('No file exists - Creating new result file and appending at {}.'.format(datetime.now()))

    with open("event_log.json", "r") as f:
        obj = json.loads(f.read())   
        
        for key, evt in obj.items():
            if key == "Events":
                
                for evt_id in obj['Events']:  
                    try:
                        #TODO: refactoring needed: in try/catch, remove testarray. To be safely implemented after next clean events_log
                          
                        if evt_id not in testarray:  

                            currEvt = obj['Events'][evt_id]

                            try:
                                commences = float(obj['Events'][evt_id]['commences'])
                                date_added = datetime.utcfromtimestamp(commences)
                            except:
                                commences = 0
                            
                            # if this event has taken place, mark in results_log.txt file
                            if datetime.now() > date_added and commences != 0: 

                                totals_points = 'None'
                                spreads_points = 'None'
                                
                                if currEvt['hasTotals'] == True:
                                    totals_points = currEvt['Odds']['totals']['totalOverPoints']
                                if currEvt['hasSpreads'] == True: 
                                    spreads_points = currEvt['Odds']['spreads']['spreadOverPoints']
                                
                                event_id = currEvt['eventid']
                                internal_id = currEvt['id']
                                home_team = currEvt['homeTeam'].encode('utf8') 
                                away_team = currEvt['awayTeam'].encode('utf8') 
                                myStr = 'Event id: {} Internal id: {}\nTeams {} vs. {}\nTotals points: {} Spread points: {}\nCommenced: {} ({}) Added to results: {}\n'.format(event_id, internal_id, home_team, away_team, totals_points, spreads_points, date_added, commences, time.time())
                                log('Adding event id {} (internal id: {}) to results file.'.format(event_id,internal_id))
                              
                                with open("results_log.txt", "a") as file:
                                    file.write('\n{}'.format(myStr))

                                to_remove.append([internal_id, event_id])

                    except Exception as e:
                            print('Error removing event from event array {}'.format(e))

    if len(to_remove) == 0:
        log('No outdated events found.')
        return

    with open("event_log.json", "r") as f:
        obj = json.loads(f.read())  
        for remove_el in to_remove: 
            log('\nAttempting to remove event id {} ({})...'.format(remove_el[0], remove_el[1]))
            if remove_el[0] in obj['Events']:
                log('Event with id {} ({}) has taken place at {}. Removing from events file and adding to results file.'.format(remove_el[0],remove_el[1], date_added))
                del obj['Events'][remove_el[0]]
                writeToFile(obj)
                result_count = result_count + 1
            else:
                log('Attempted to remove id {} from file but failed.'.format(remove_el[0]))
  
# Print log to debug_log.txt and console
def log(content):
    
    print (content)
    
    if os.path.exists("debug_log.txt") == False:
        print('No file exists - Creating new debug file and appending at {}.'.format(datetime.now()))
        c = open("debug_log.txt","w+")
        c.write('No file exists - Creating new debug file and appending at {}.'.format(datetime.now()))

    if os.path.exists("results_log.txt") == False:
        print('No results_log file exists - Creating new results_log file at {}.'.format(datetime.now()))
        c = open("results_log.txt","w+")

    with open("debug_log.txt", "a") as file:
        file.write('\n {}'.format(content))

# Use minimum needed UTXOs from unspent to use towards transaction
def getRequiredTxList (utxo_list, amtRequired):

    amt = 0; txList = '['
    utxoJSON = json.loads(utxo_list)
    amtRequired += 1
    returnArray = {}

    for tx in utxoJSON:
        txString = "{\"txid\":\"" + tx["txid"]+"\",\"vout\":" + str(tx["vout"]) + ", \"address\":\"" + tx["address"]+"\",\"account\":\"""\",\"scriptPubKey\":\"" + tx["scriptPubKey"]+"\",\"amount\":" + str(tx["amount"]) + ",\"confirmations\":" + str(tx["confirmations"]) + ",""\"spendable\":" + str(tx["spendable"]).lower() + "}"

        txList+=txString
        amt = float(tx['amount']) + float(amt)
        if amt >= float(amtRequired):
            txList+=']'
            returnArray['txList'] = txList
            returnArray['amt'] = amt
            return returnArray
        else:
            txList+=','

# Capitalise and truncate strings, limit to 50 characters
def standardiseText (text):
    
    try:
        text = str(text).title().decode('utf-8')
    except Exception as e:
        log('Could not decode and standardise string: {}, Error {}'.format(text, e))
        return text
    return text[:50]

# Remove floating point number from odds
def calibrateOdds (home, away, draw):

    totalProb = 0

    if draw == 0:
        odds = [home, away]
    else:
        odds = [home, away, draw]

    calOdds = []
    for i in odds:
        newOdds = 1000000/i
        calOdds.append(newOdds)
        totalProb = totalProb + newOdds

    calibratedOdds = {}
    calTotalProb = 100000000/totalProb
    calibratedOdds[0] = int((float(odds[0])/calTotalProb)*1000000)
    calibratedOdds[1] = int((float(odds[1])/calTotalProb)*1000000)

    if draw == 0:
        calibratedOdds[2] = 0
    else:
        calibratedOdds[2] = int((float(odds[2])/calTotalProb)*1000000)

    return calibratedOdds

# Check if the main thread is running. If not, restart and notify admin of any errors
def threadCheck ():

    global last_event_post, last_result_write, last_results_email, sent_first_error, sent_first_fatal_error, first_event_posted, get_events_thread
    global result_count

    since_last_event = (datetime.now()- last_event_post).total_seconds()
    since_last_results_email = (datetime.now()- last_results_email).total_seconds()
    since_last_results_write = (datetime.now()- last_result_write).total_seconds()
    since_last_activity = (datetime.now()- last_activity).total_seconds()
    
    if first_event_posted == True:
        print('\nThread check {}: It has been {} seconds since the last event was posted.'.format(datetime.now(), since_last_event))
    else:
        print('\nThread check {}: No events have been posted since script start.'.format(datetime.now()))
        
    # Check that events are being regularly updated, restart thread if dead
    if since_last_event > events_inactivity_allowance:       
        log('ERROR: An event has not been posted events for an irregular amount of time.')
        notifyAdmin('Timeout Error', since_last_event)
        
        try:   
            log("Checking if main thread need to be restarted...")
            if not get_events_thread.thread.is_alive() and since_last_activity > any_inactivity_allowance:
                get_events_thread.cancel()
                get_events_thread = perpetualTimer(check_events_interval, getOddsAPIEvents) 
                get_events_thread.start()
                log('Main thread has been restarted')
            return
        except Exception as e:
            log('{}'.format(e))

    # Remove outdated events from events log
    if since_last_results_write > write_results_interval:
        writeToResultsFile()

    # Send results log to admin after specified period
    if since_last_results_email > post_results_email_interval and result_count >= results_after:
        notifyAdmin('Results', since_last_event)
    else:
        log('Results will be updated after {} new results, current count is {}'.format(results_after, result_count))

# Notify admin if no event or result has been posted for a period of time
def notifyAdmin (notification, since_last_event):

    global last_error_email, last_fatal_error_email, sent_first_error, sent_first_fatal_error, sent_first_results_file,last_results_email
    global no_results_before_email, result_count

    since_last_error_email = (datetime.now()- last_error_email).total_seconds()
    since_last_fatal_error_email = (datetime.now()- last_fatal_error_email).total_seconds()

    body = ''
    msg = "Autoposting Error: - last event sent at  {}".format(time.strftime("%M:%S", time.gmtime(since_last_event)))
    msg = MIMEMultipart()
    msg['From'] = admin_from_address
    msg['To'] = admin_to_address
    recentTxt = '-> most recent 500 lines of text within the referenced file...\n'
    
    # If a fatal error is recieved, send debug email to admin immediately
    if notification == 'Fatal Error':       
        if since_last_fatal_error_email > post_fatal_error_email_interval or sent_first_fatal_error == False:          
            with open("debug_log.txt") as f:
                for row in f.readlines()[-500:]:
                    recentTxt += row
            try: 
                msg['Subject'] = "Autoposting - Fatal Error"
                last_fatal_error_email = datetime.now()
                log('Fatal error email sent to ({}) - next email in {}'.format(admin_to_address, post_fatal_error_email_interval))
                body = MIMEText(recentTxt)
                msg.attach(body)
                server = smtplib.SMTP(admin_provider, 587)
                server.ehlo()
                server.starttls()
                server.login(admin_from_address, admin_from_pass)
                text = msg.as_string()
                server.sendmail(admin_from_address, admin_to_address, text)
                sent_first_fatal_error = True

                body = MIMEText(recentTxt)
                msg.attach(body)
                server = smtplib.SMTP(admin_provider, 587)
                server.ehlo()
                server.starttls()
                server.login(admin_from_address, admin_from_pass)
                text = msg.as_string()
                server.sendmail(admin_from_address, error_to_address, text)

                return
            except Exception as e:
                    print("Could not send email {}".format(e))
        else:
            log('Fatal error email will be sent in {} seconds if script remins inactive'.format(since_last_fatal_error_email - since_last_fatal_error_email))
    
    # Send last 500 lines of text from debug log to admin address fter timeout error
    if notification == 'Timeout Error': 
        if since_last_error_email > post_error_email_interval or sent_first_error == False:
            try: 
                with open("debug_log.txt") as f:
                    for row in f.readlines()[-500:] :
                        recentTxt += row
                msg['Subject'] = "Autoposting - Timeout Error"
                last_error_email = datetime.now()
                log('Timeout error sent to ({}) - next email in {} seconds'.format(admin_to_address, post_fatal_error_email_interval))       
                body = MIMEText(recentTxt)
                msg.attach(body)
                server = smtplib.SMTP(admin_provider, 587)
                server.ehlo()
                server.starttls()
                server.login(admin_from_address, admin_from_pass)
                text = msg.as_string()
                server.sendmail(admin_from_address, admin_to_address, text)
                sent_first_error = True

                body = MIMEText(recentTxt)
                msg.attach(body)
                server = smtplib.SMTP(admin_provider, 587)
                server.ehlo()
                server.starttls()
                server.login(admin_from_address, admin_from_pass)
                text = msg.as_string()
                server.sendmail(admin_from_address, error_to_address, text)

                return

            except Exception as e:
                print("Could not send email {}".format(e))
        else:
            log('Timeout error email will be sent in {} seconds if script remins inactive.'.format(post_error_email_interval - since_last_error_email))
   
    elif notification == 'Results':

        try:
            with open("results_log.txt") as f:
                for row in f.readlines()[-500:] :
                    recentTxt += row
            msg['Subject'] = "Autoposting - Results Log"
            log('Results sent to {} - will be resent in {} seconds.'.format(admin_to_address, post_results_email_interval))  
            body = MIMEText(recentTxt)
            msg.attach(body)
            server = smtplib.SMTP(admin_provider, 587)
            last_error_email = datetime.now()
            server.ehlo()
            server.starttls()
            server.login(admin_from_address, admin_from_pass)
            text = msg.as_string()
            server.sendmail(admin_from_address, admin_to_address, text)

            body = MIMEText(recentTxt)
            msg.attach(body)
            server = smtplib.SMTP(admin_provider, 587)
            server.ehlo()
            server.starttls()
            server.login(admin_from_address, admin_from_pass)
            text = msg.as_string()
            server.sendmail(admin_from_address, 'brian@techsquad.io', text)

            sent_first_results_file = True
            last_results_email = datetime.now()
            result_count = 0

        except Exception as e:
            log('Error sending Results log file via email {}'.format(e))

# Create opCode for Results event and add to blockchain *** UNUSED ****
def createResultOpCode():
    
    match_result = 2 
    event_id = 700

    result_tx_typeHX = '{:02x}'.format(int(result_tx_type))
    version_numberHX = '{:02x}'.format(int(version_number))
    evtIdHX = '{:02x}'.format(event_id)
    match_resultHX = '{:02x}'.format(match_result)
    match_resultHX = str(match_resultHX).zfill(2)
    evtIdHX = str(evtIdHX).zfill(8)

    # Create OP CODE from result data
    op_code = ""+opcode_prefix+version_numberHX+result_tx_typeHX+evtIdHX+match_resultHX+""
    log ("\n *** ({}/{}) Creating result ***\nOpcode: {} ".format(event_id, event_id, op_code))
    postOpCode(op_code, 'newResult', event_id)

    return

# Retrieve and post event/result data at intervals
def main ():
    
    global get_events_thread, get_results_thread, last_saved_event_id, result_count

    result_count = 0
    #createResultOpCode ()
    last_saved_event_id = getCurrentGame()

    writeToResultsFile()
    
    # Retrieve and validate new events from API and post to chain
    get_events_thread = perpetualTimer(check_events_interval, getOddsAPIEvents)
    getOddsAPIEvents()

    # Retrieve and validate new results from API and post to chain
    thread_checker = perpetualTimer(check_thread_interval,threadCheck)
    threadCheck()

    # Start all threads
    get_events_thread.start()
    thread_checker.start()

main()