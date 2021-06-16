import json
import collibra
import db
import globals
import util
import sys

DEBUG = False

class Asset:
    Total = 0
    def __init__(self, _domain_id:str, _id:str, _name:str, _lastmodified:int, _status:str):
        self.domain_id = _domain_id
        self.name = _name.strip()
        self.id = _id
        self.last_synced = globals.CURRENT_EPOCH_TIMESTAMP
        self.dataAttributes = ""
        self.collibra_status = 0
        self.num_of_fields = 0
        self.lastModified = _lastmodified

    def set_attributes(self, _attribs):
        self.dataAttributes = _attribs

    def set_total_fields(self, _count):
        self.num_of_fields = _count

    def _print(self):
        print('domain: {} \nasset-key: {} \nattribs: {} \nis active:{} \nlast synced:{} \nNum of Fields: {} \nLast Modified: {}'.format(self.domain_id
                        , self.id
                        , self.name
                        , self.dataAttributes
                        , self.is_active
                        , self.last_synced
                        , self.num_of_fields
                        , self.lastModified
        ))

def process_raw_assets(domain_id:str):
    raw = util.read_from_file(domain_id + '.json', globals.DIR_LOG01)

    if raw["results"] is None:
        sys.exit("raw json data is empty")
    pass

    LST_ASSETS = []

    _key = ""
    _value = ""
    _lastmod = 0

    for el in raw["results"]:
        for k,v in el["resource"].items():
            if k == 'id':
                _key = v
            elif k == 'name':
                _value = str(v).strip()
            elif k == 'lastModifiedOn':
                _lastmod = v
            elif k == 'status':
                _status = el["resource"]["status"]["id"]
                if _status == globals.STATUS_CANDIDATE:
                    _status = "Candidate"
                elif _status == globals.STATUS_APPROVED:
                    _status = "Approved"
                elif _status == globals.STATUS_OBSOLETE:
                    _status = "Obsolete"

        LST_ASSETS.append(Asset(globals.DOMAIN_ID, _key, _value, _lastmod, _status))

    print('A List of assets has been created.')
    return LST_ASSETS

def process_empty_name_rows():
    select_insert_sql = """
        INSERT INTO "DataAssetsTable2"("AssetKey", "Name", "DomainKey", "NumofFields", "DataAttributes", "CollibraStatus", "LastSynced", "CollibraLastModifiedOn")
        SELECT "AssetKey", "Name", "DomainKey", "NumofFields", "DataAttributes", "CollibraStatus", "LastSynced", "CollibraLastModifiedOn" FROM DataAssetsTable1
        WHERE "Name"=''

        """
    db.upsert(select_insert_sql, "public")

    delete_sql = """
        DELETE FROM DataAssetsTable1
        WHERE "Name"=''

        """
    db.upsert(select_insert_sql, "asset")

if __name__ == "__main__":
    clb = collibra.Collibra(globals.COLLIBRA_UID, globals.COLLIBRA_PWD)
    clb.get_latest_assets(globals.DOMAIN_ID)
    assets = process_raw_assets(globals.DOMAIN_ID)

    for a in assets:
        response = clb.get_attributes_by_assets(a.id)
        json_response = json.loads(response)

        processed_data = collibra.Collibra.get_key_value_from_attributes(json_response["results"])
        a.set_attributes(processed_data)
        a.set_total_fields(len(processed_data))

        util.save_to_file(response, a.id, + 'json', globals.DIR_LOG02)
        print("{}.json has been saved".format(a.id))

    sql_statement = """
        INSERT INTO "DataAssets"("AssetKey", "Name", "DomainKey", "NumofFields", "DataAttributes", "CollibraStatus", "LastSynced", "CollibraLastModifiedOn")
        VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{4}', '{6}', '{7}')
        ON CONFLICT ("AssetKey")
        DO
            UPDATE SET "Name" = '{1}'
                , "DomainKey" = '{2}'
                , "NumofFields" = {3}
                , "DataAttributes" = '{4}'
                , "CollibraStatus" = '{5}'
                , "LastSynced" = {6}
                , "CollibraLastModifiedOn" = {7};

        """

    for a in assets:
        util.save_to_file(json.dumps(a.dataAttributes), a.id + '.json', globals.DIR_LOG03)
        sql_param = sql_statement.format(a.id, a.name, a.domain_id, a.num_of_fields, json.dumps(a.dataAttributes), a.Collibra_status, a.last_synced, a.lastModified)
        if DEBUG:
            print(sql_param)
        else:
            db.upsert(sql_param, "asset")

    process_empty_name_rows()




            