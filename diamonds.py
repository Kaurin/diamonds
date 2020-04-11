import zlib
import zmq
import simplejson
import boto3
import sys, os, datetime, time
import rfc3339
import iso8601

session = boto3.Session(region_name="eu-west-1")
sns_client = session.client("sns")

"""
 "  Configuration
"""
__relayEDDN = "tcp://eddn.edcd.io:9500"
__timeoutEDDN = 600000
__recieveText = False
__mobileNum = "+1CHANGEME"
__minPrice = 1
__minDemand = 1
__stationMap = {}
__savedStationsExpiryHours = 6

# Set False to listen to production stream
__debugEDDN = False

# A sample list of authorised softwares
__authorisedSoftwares = [
    "E:D Market Connector",
    "EDDiscovery",
    "EDDI",
    "EDCE",
    "ED-TD.SPACE",
    "EliteOCR",
    "Maddavo's Market Share",
    "RegulatedNoise",
    "RegulatedNoise__DJ",
    "E:D Market Connector [Windows]",
    "E:D Market Connector [Linux]",
    "EliteLogAgent",
]

# Used this to excludes yourself for example has you don't want to handle your own messages ^^
__excludedSoftwares = ["My Awesome Market Uploader"]


"""
 "  Start
"""


def sendSMS(message):
    response = sns_client.publish(
        PhoneNumber=__mobileNum,
        Message=message,
        MessageAttributes={
            "AWS.SNS.SMS.SenderID": {"DataType": "String", "StringValue": "Diamonds",},
            "AWS.SNS.SMS.SMSType": {
                "DataType": "String",
                "StringValue": "Transactional",
            },
        },
    )
    print(response)


def get_date_object(date_string):
    return iso8601.parse_date(date_string)


def get_date_string(date_object):
    return rfc3339.rfc3339(date_object)


def shouldIText(message):
    uniqueName = message["systemName"] + "__" + message["stationName"]
    timestamp = get_date_object(message["timestamp"])
    if uniqueName not in __stationMap:
        __stationMap[uniqueName] = timestamp
        return True
    fetchedDateTime = __stationMap.get(uniqueName)
    if fetchedDateTime + datetime.timedelta(
        hours=__savedStationsExpiryHours
    ) < datetime.datetime.now(datetime.timezone.utc):
        __stationMap[uniqueName] = timestamp
        return True

    return False


def main():
    print("Sending test SMS")
    sendSMS("Started EDDN Subscriber")
    print("Starting EDDN Subscriber")
    print("")

    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)

    subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    subscriber.setsockopt(zmq.RCVTIMEO, __timeoutEDDN)

    while True:
        try:
            subscriber.connect(__relayEDDN)
            print("Connect to " + __relayEDDN)
            print("")
            print("")

            while True:
                __message = subscriber.recv()

                if __message == False:
                    subscriber.disconnect(__relayEDDN)
                    print("Disconnect from " + __relayEDDN)
                    print("")
                    print("")
                    break

                # print("Got a message")

                __message = zlib.decompress(__message)
                if __message == False:
                    print("Failed to decompress message")

                __json = simplejson.loads(__message)
                if __json == False:
                    print("Failed to parse message as json")

                __converted = False

                # Handle commodity v1
                if __json[
                    "$schemaRef"
                ] == "https://eddn.edcd.io/schemas/commodity/1" + (
                    "/test" if (__debugEDDN == True) else ""
                ):
                    print("Receiving commodity-v1 message...")
                    print("    - Converting to v3...")

                    __temp = {}
                    __temp["$schemaRef"] = (
                        "https://eddn.edcd.io/schemas/commodity/3"
                        + ("/test" if (__debugEDDN == True) else "")
                    )
                    __temp["header"] = __json["header"]

                    __temp["message"] = {}
                    __temp["message"]["systemName"] = __json["message"]["systemName"]
                    __temp["message"]["stationName"] = __json["message"]["stationName"]
                    __temp["message"]["timestamp"] = __json["message"]["timestamp"]

                    __temp["message"]["commodities"] = []

                    __commodity = {}

                    if "itemName" in __json["message"]:
                        __commodity["name"] = __json["message"]["itemName"]

                    if "buyPrice" in __json["message"]:
                        __commodity["buyPrice"] = __json["message"]["buyPrice"]
                    if "stationStock" in __json["message"]:
                        __commodity["supply"] = __json["message"]["stationStock"]
                    if "supplyLevel" in __json["message"]:
                        __commodity["supplyLevel"] = __json["message"]["supplyLevel"]

                    if "sellPrice" in __json["message"]:
                        __commodity["sellPrice"] = __json["message"]["sellPrice"]
                    if "demand" in __json["message"]:
                        __commodity["demand"] = __json["message"]["demand"]
                    if "demandLevel" in __json["message"]:
                        __commodity["demandLevel"] = __json["message"]["demandLevel"]

                    __temp["message"]["commodities"].append(__commodity)
                    __json = __temp
                    del __temp, __commodity

                    __converted = True

                # Handle commodity v3
                if __json[
                    "$schemaRef"
                ] == "https://eddn.edcd.io/schemas/commodity/3" + (
                    "/test" if (__debugEDDN == True) else ""
                ):
                    if __converted == False:
                        # print("Receiving commodity-v3 message...")
                        pass

                    __authorised = False
                    __excluded = False

                    if __json["header"]["softwareName"] in __authorisedSoftwares:
                        __authorised = True
                    if __json["header"]["softwareName"] in __excludedSoftwares:
                        __excluded = True

                    if not __authorised:
                        print(
                            "    - Software: "
                            + __json["header"]["softwareName"]
                            + " / "
                            + __json["header"]["softwareVersion"]
                        )
                        print("EXCLUDED" if (__excluded == True) else "UNAUTHORISED")

                    if __authorised == True and __excluded == False:
                        for __commodity in __json["message"]["commodities"]:
                            if __commodity["name"] != "lowtemperaturediamond":
                                continue
                            if __commodity["demand"] < __minDemand:
                                continue
                            if __commodity["sellPrice"] < __minPrice:
                                continue
                            print(
                                "        - System Name: "
                                + __json["message"]["systemName"]
                            )
                            print(
                                "        - Station Name: "
                                + __json["message"]["stationName"]
                            )
                            print(
                                "                - Sell Price: "
                                + str(__commodity["sellPrice"])
                            )
                            print(
                                "                - Demand: "
                                + str(__commodity["demand"])
                                + (
                                    (" (" + __commodity["demandLevel"] + ")")
                                    if "demandLevel" in __commodity
                                    else ""
                                )
                            )
                            print(shouldIText(__json["message"]))
                            if __recieveText:
                                sendSMS(
                                    f'System: {__json["message"]["systemName"]}, Station: {__json["message"]["stationName"]}, SellPrice: {__commodity["sellPrice"]}, Demand: {__commodity["demand"]}'
                                )
                                pass
                            break
                        # End example

                    del __authorised, __excluded
                else:
                    # print("Unknown schema: " + __json["$schemaRef"])
                    pass

                del __converted

        except zmq.ZMQError as e:
            sendSMS("Something went wrong!")
            print("")
            print("ZMQSocketException: " + str(e))
            subscriber.disconnect(__relayEDDN)
            print("Disconnect from " + __relayEDDN)
            print("")
            time.sleep(5)


if __name__ == "__main__":
    main()
