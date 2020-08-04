"""
Project # 1 
Name:
Time to completion:
Comments:

Sources:
"""

# You are welcome to add code outside of functions
# like imports and other statements

import csv
import json
import xml.etree.ElementTree as ET


def read_csv_string(input_):
    
    count = 0
    return_dict = {}
    rows = []
    new = input_.split("\n")
    new2 = new[count].split(",")
    count += 1
    for i in new2:
        return_dict[i] = []
    for i in new:
        new2 = new[count].split(",")
        count+=1
        if new2[0] == '':
            break
        rows.append(new2)
    col_count = 0
    for k in return_dict.keys():
        for row in rows:
            return_dict[k].append(row[col_count])
        col_count += 1
    return return_dict
    


def write_csv_string(data):
    return_string = ''
    limit = len(data)-1
    for v in data.values():
        limit2 = len(v)
        break
    count = 0
    count2 = 0
    count3 = 0
    for k in data.keys():
        if count != limit:
            return_string += (k + ',')
            count += 1
            continue
        count3+=1
        return_string += (k + '\n')
    while count2 != limit2:
        count = 0
        for v in data.values():
            if count != limit:
                return_string += (v[count2] + ',')
                count += 1
                continue
            return_string += (v[count2] + '\n')
        count2 += 1
        
    return return_string


def read_json_string(input_):
    y = json.loads(input_)
    col_names = []
    return_dict = {}
    for k in y[0].keys():
        return_dict[k] = []
    for dict_ in y:
        for k,v in dict_.items():
            return_dict[k].append(v)
    return return_dict
        
    
def write_json_string(data):
    return_string = "["
    keys = []
    values = []
    i = 0
    key_tracker = 0
    value_tracker = 0
    num_of_keys = len(data.keys())
    for k,v in data.items():
        keys.append(k)
        values.append(v)
        length = len(v)
    val_length = len(values)
    return_string += "{"
    while i != length:
        count = 0
        while count != num_of_keys:
            return_string += '\"'
            return_string += keys[key_tracker]
            key_tracker += 1
            if key_tracker == num_of_keys:
                key_tracker = 0
            return_string += '\": \"'
            return_string += values[count][value_tracker]
            count += 1
            return_string += '\", '
        return_string = return_string[0:-2]
        return_string+='}, {'
        value_tracker += 1
        i+=1
    return_string = return_string[0:-3]
    return_string+="]"
    return return_string

    


def read_xml_string(input_):
    return_dict = {}
    tree = ET.fromstring(input_)
    add  = ""
    added = []
    add_flag = False
    y = 0
    thing = ""
    for child in tree:
        y+=1
        x = len(child)
        for c in child:
            return_dict[c.tag] = []
    for i in input_:
        if i == "&":
            thing += "&"
            continue
        if thing == "&" and i == "l":
            thing+="l"
            continue
        if thing == "&l" and i == "t":
            thing+="t"
            continue
        if thing == "&lt" and i == ";":
            add+= "<"
            thing = ""
            continue
        if thing == "&" and i == "g":
            thing+="g"
            continue
        if thing == "&g" and i == "t":
            thing+="t"
            continue
        if thing == "&gt":
            add+= ">"
            thing = ""
            continue
        if i == ">" and add_flag != True:
            add_flag = True
            continue
        if add_flag == True and i != "<" and i != '>':
            add += i
            continue
        if add_flag == True and i == "<" and i != '>':
            if add != '':
                added.append(add)
                add = ""
            add_flag = False
    new_list = []
    v_count = 0
    for i in range(y):
        i = 0
        for k in return_dict.keys():
            return_dict[k].append(added[i+(v_count*x)])
            i += 1
        v_count+=1
   
    return return_dict
            


def write_xml_string(data):
    return_string = "<data>"
    for k in data.keys():
        length = len(data[k])
        break
    for i in range(length):
        new = ""
        return_string+= ("<record>")
        for k,v in data.items():
            return_string+= ("<" + k + ">")
            if ">" not in data[k][i] and "<" not in data[k][i]:
                return_string+=data[k][i]
            if ">" in data[k][i] or "<" in data[k][i]:
                for x in data[k][i]:
                    if x != ">" and x != "<":
                        new += x
                    if x == ">":
                        new += "&gt;"
                    if x == "<":
                        new += "&lt;"
                print(new)
                return_string += new
                        

            return_string+= ("</" + k + ">")
        return_string += "</record>"
    return_string += "</data>"
    return return_string



# The code below isn't needed, but may be helpful in testing your code.
if __name__ == "__main__":
    input_ = """street,city,zip,state,beds,baths,sq__ft,type,sale_date,price,latitude,longitude
        3526 HIGH ST,SACRAMENTO,95838,CA,2,1,836,Residential,Wed May 21 00:00:00 EDT 2008,59222,38.631913,-121.434879
        51 OMAHA CT,SACRAMENTO,95823,CA,3,1,1167,Residential,Wed May 21 00:00:00 EDT 2008,68212,38.478902,-121.431028
        2796 BRANCH ST,SACRAMENTO,95815,CA,2,1,796,Residential,Wed May 21 00:00:00 EDT 2008,68880,38.618305,-121.443839
        2805 JANETTE WAY,SACRAMENTO,95815,CA,2,1,852,Residential,Wed May 21 00:00:00 EDT 2008,69307,38.616835,-121.439146
        6001 MCMAHON DR,SACRAMENTO,95824,CA,2,1,797,Residential,Wed May 21 00:00:00 EDT 2008,81900,38.51947,-121.435768
        5828 PEPPERMILL CT,SACRAMENTO,95841,CA,3,1,1122,Condo,Wed May 21 00:00:00 EDT 2008,89921,38.662595,-121.327813
        6048 OGDEN NASH WAY,SACRAMENTO,95842,CA,3,2,1104,Residential,Wed May 21 00:00:00 EDT 2008,90895,38.681659,-121.351705
        2561 19TH AVE,SACRAMENTO,95820,CA,3,1,1177,Residential,Wed May 21 00:00:00 EDT 2008,91002,38.535092,-121.481367
        11150 TRINITY RIVER DR Unit 114,RANCHO CORDOVA,95670,CA,2,2,941,Condo,Wed May 21 00:00:00 EDT 2008,94905,38.621188,-121.270555 """

    expected = """street,city,zip,state,beds,baths,sq__ft,type,sale_date,price,latitude,longitude
        3526 HIGH ST,SACRAMENTO,95838,CA,2,1,836,Residential,Wed May 21 00:00:00 EDT 2008,59222,38.631913,-121.434879
        51 OMAHA CT,SACRAMENTO,95823,CA,3,1,1167,Residential,Wed May 21 00:00:00 EDT 2008,68212,38.478902,-121.431028
        2796 BRANCH ST,SACRAMENTO,95815,CA,2,1,796,Residential,Wed May 21 00:00:00 EDT 2008,68880,38.618305,-121.443839
        2805 JANETTE WAY,SACRAMENTO,95815,CA,2,1,852,Residential,Wed May 21 00:00:00 EDT 2008,69307,38.616835,-121.439146
        6001 MCMAHON DR,SACRAMENTO,95824,CA,2,1,797,Residential,Wed May 21 00:00:00 EDT 2008,81900,38.51947,-121.435768
        5828 PEPPERMILL CT,SACRAMENTO,95841,CA,3,1,1122,Condo,Wed May 21 00:00:00 EDT 2008,89921,38.662595,-121.327813
        6048 OGDEN NASH WAY,SACRAMENTO,95842,CA,3,2,1104,Residential,Wed May 21 00:00:00 EDT 2008,90895,38.681659,-121.351705
        2561 19TH AVE,SACRAMENTO,95820,CA,3,1,1177,Residential,Wed May 21 00:00:00 EDT 2008,91002,38.535092,-121.481367
        11150 TRINITY RIVER DR Unit 114,RANCHO CORDOVA,95670,CA,2,2,941,Condo,Wed May 21 00:00:00 EDT 2008,94905,38.621188,-121.270555 """

    def super_strip(input_):
        """
        Removes all leading/trailing whitespace and blank lines
        """
        lines = []
        for line in input_.splitlines():
            stripped = line.strip()
            if stripped:
                lines.append(stripped)
        return "\n".join(lines) + "\n"

    input_ = super_strip(input_)
    expected = super_strip(expected)

    print("Input:")
    print(input_)
    print()
    data = read_xml_string(input_)
    print("Your data object:")
    print(data)
    print()
    output = write_csv_string(data)
    output = super_strip(output)
    print("Output:")
    print(output)
    print()
