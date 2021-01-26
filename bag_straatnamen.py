import os
import xml.etree.ElementTree as ET
import pandas as pd

ns = {
    "DatatypenNEN3610":"www.kadaster.nl/schemas/lvbag/imbag/datatypennen3610/v20200601"
    ,"Objecten":"www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601"
    ,"gml":"http://www.opengis.net/gml/3.2"
    ,"Historie":"www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601"
    ,"Objecten-ref":"www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601"
    ,"nen5825":"www.kadaster.nl/schemas/lvbag/imbag/nen5825/v20200601"
    ,"KenmerkInOnderzoek":"www.kadaster.nl/schemas/lvbag/imbag/kenmerkinonderzoek/v20200601"
    ,"selecties-extract":"http://www.kadaster.nl/schemas/lvbag/extract-selecties/v20200601"
    ,"sl-bag-extract":"http://www.kadaster.nl/schemas/lvbag/extract-deelbestand-lvc/v20200601"
    ,"sl":"http://www.kadaster.nl/schemas/standlevering-generiek/1.0"
    ,"xsi":"http://www.w3.org/2001/XMLSchema-instance"
    ,"xs":"http://www.w3.org/2001/XMLSchema"
    ,"xsi:schemaLocation":"http://www.kadaster.nl/schemas/lvbag/extract-deelbestand-lvc/v20200601 http://www.kadaster.nl/schemas/bag-verstrekkingen/extract-deelbestand-lvc/v20200601/BagvsExtractDeelbestandExtractLvc-2.1.0.xsd"
}

def get_addresses(path):
    parser = ET.XMLParser(encoding="utf-8")
    root = ET.parse(path, parser=parser).getroot()
    items = root.findall('sl:standBestand/sl:stand', ns)
    print(f'Addresses in file: {len(items)}')
    return items

def parse_streetnames(input_path):
    parser = ET.XMLParser(encoding="utf-8")
    root = ET.parse(input_path, parser=parser).getroot()
    items = root.findall('sl:standBestand/sl:stand', ns)
    print(f'Addresses in file: {len(items)}')
    out_list = []
    for index, address in enumerate(items):
        straat = address.find('sl-bag-extract:bagObject/Objecten:OpenbareRuimte/Objecten:naam', ns).text.strip()
        try:
            straat_verkort = address.find('sl-bag-extract:bagObject/Objecten:OpenbareRuimte/Objecten:verkorteNaam/nen5825:VerkorteNaamOpenbareRuimte/nen5825:verkorteNaam', ns).text.strip()
        except Exception:
            straat_verkort = None
        out_line = {
            'straat': straat,
            'straat_verkort': straat_verkort
        }
        out_list.append(out_line)
    return pd.DataFrame(out_list)

def batch_parse_save(folder_path):
    files = [folder_path + f for f in os.listdir(folder_path)]
    parsed = []
    for file_path in files:
        parsed.append(parse_streetnames(file_path))
    all = pd.concat(parsed, ignore_index=True).drop_duplicates()
    all.to_csv('straatnamen_parsed.csv', index=False, sep='|')
    return all

batch_parse_save(folder_path='9999OPR08012021/')
