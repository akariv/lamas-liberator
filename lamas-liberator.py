import mechanize
import pprint
import lxml.etree as ET
import lxml.html as lh
import urllib
import urllib2
import csv
import sys

class scraper:
    def __init__(self):
        self.browser=mechanize.Browser()
        self.data = []
        self.columns = set([])
        self.downloaded = 0
        self.max_download = None

    def dump(self,filename):
        self.columns = list(self.columns)
        self.columns.sort()
        self.columns.insert(0, 'series')
        f = open(filename,'wt')
        w = csv.DictWriter(f, self.columns)
        w.writeheader()
        for d in self.data:
            w.writerow(d)
        f.close()

    def scrape_category(self, category_num):
        self.browser.open("http://www.cbs.gov.il/ts/databank/building_func_e.html?level_1=%d"
                          % category_num)
        content = self.browser.response().read()
        doc = lh.fromstring(content)
        urls = [li.attrib['onclick'].split("'")[1] for li in doc.xpath('//li[@onclick]')]
        for url in urls:
            self.parse_url("%s%s" % ("http://www.cbs.gov.il/ts/databank/", url))
            print "collected %d series" % self.downloaded
        print "done"

    def parse_url(self,url):
        self.browser.open(url)
        self.parse_form()

    def parse_form(self, form_number=0, level=0):
        if self.max_download and (self.downloaded > self.max_download):
            return
        try:
            self.browser.select_form(nr=form_number)
        except mechanize.FormNotFoundError:
            content = self.browser.response().read()
            print "%s  content length %d" % ('    '*level, len(content))
            doc=lh.fromstring(content)
            params=dict((elt.attrib['name'],elt.attrib['value']) for elt in doc.xpath('//input[@type="hidden"]'))
            params['king_format']=2
            url='http://www.cbs.gov.il/ts/databank/data_ts_format_e.xml'
            params=urllib.urlencode(dict((p,params[p]) for p in params.keys() if
                                         p in [ 'king_format', 'tod', 'time_unit_list',
                                                'mend', 'yend', 'co_code_list',
                                                'name_tatser_list', 'ybegin', 'mbegin',
                                                'code_list', 'co_name_tatser_list', 'level_1',
                                                'level_2', 'level_3']))
            self.browser.open(url+'?'+params)
            content = self.browser.response().read()
            print "%s  xml content length %d" % ('    '*level, len(content))
            content = content.replace('iso-8859-8-i','iso-8859-8')
            doc = ET.fromstring(content)
            for series in doc.xpath('/series_ts/Data_Set/Series'):
                #print(series.attrib)
                d = {'series': str(series.attrib)}
                for elt in series.xpath('obs'):
                    year = int(elt.attrib['time_period'])
                    if elt.attrib['value']:
                        value = float(elt.attrib['value'])
                    else:
                        value = None
                    d[year] = value
                    self.columns.add(year)
                self.data.append(d)
                self.downloaded += 1
            self.browser.back()
            return

        selects = [x.type=='select' for x in self.browser.form.controls]

        control_name = self.browser.form.controls[-2].attrs['name']
        if control_name in ['ybegin','yend']:
            self.browser.submit()
            self.parse_form(form_number+1, level+1)
            self.browser.back()
            return
        print "%schanging control %s" % ('    '*level, control_name)
        for option in self.browser.form.controls[-2].items:
            print "%s  option %s - %s" % ('    '*level,
                                          option.name,
                                          option.attrs['label'])
            self.browser.form[control_name] = [option.name]
            self.browser.submit()
            self.parse_form(form_number+1, level+1)
            self.browser.back()
            self.browser.select_form(nr=form_number)
        return

if __name__ == "__main__":
    try:
        category = int(sys.argv[1])
    except:
        print "run %s category_id " % sys.argv[0]
        print "(category_id should be a number. try 24 or 21 for example)"
        exit(1)
    x = scraper()
    x.scrape_category(category)
    x.dump('output.csv')

