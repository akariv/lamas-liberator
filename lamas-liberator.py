import mechanize
import pprint
import lxml.etree as ET
import lxml.html as lh
import urllib
import urllib2
import sys
import json
import logging
import cPickle
from logging import StreamHandler, FileHandler

L = logging.getLogger()
formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
sh = StreamHandler()
fh = FileHandler( 'run.log' )
sh.setFormatter(formatter)
fh.setFormatter(formatter)
L.addHandler( sh )
L.addHandler( fh )
L.setLevel( logging.DEBUG )

class scraper:
    def __init__(self):
        self.browser=mechanize.Browser()
        self.data = []
        self.downloaded = 0
        self.max_download = None

    def dump(self,filename):
        if len(self.data) == 0:
           L.info('empty data, not creating file')
           return

        f = open(filename,'wt')
        for d in self.data:
            try:
                f.write(json.dumps(d)+'\n')
            except:
                L.error( "bad data %r" % d )
	self.data = []
	self.downloaded = 0
        f.close()

    def scrape_category(self, category_num):
        self.browser.open("http://www.cbs.gov.il/ts/databank/building_func_e.html?level_1=%d"
                          % category_num, timeout=60)
        content = self.browser.response().read()
        doc = lh.fromstring(content)
        urls = [li.attrib['onclick'].split("'")[1] for li in doc.xpath('//li[@onclick]')]
        for url in urls:
            self.parse_url("%s%s" % ("http://www.cbs.gov.il/ts/databank/", url))
            L.info( "collected %d series" % self.downloaded )
        L.info( "%d: done" % category_num if len(urls) > 0 else "%d: nothing to do here" % category_num )

    def parse_url(self,url):
        self.browser.open(url)
        self.parse_form()

    def parse_form(self, form_number=0, level=0,slug=""):
        if self.max_download and (self.downloaded > self.max_download):
            return
        try:
            self.browser.select_form(nr=form_number)
        except mechanize.FormNotFoundError:
            content = self.browser.response().read()
            #L.info( "%s  content length %d" % ('    '*level, len(content)) )
            doc=lh.fromstring(content)
            params=dict((elt.attrib['name'],elt.attrib['value']) for elt in doc.xpath('//input[@type="hidden"]'))
            params['king_format']=2
            url='http://www.cbs.gov.il/ts/databank/data_ts_format_e.xml'
            params_query=urllib.urlencode(dict((p,params[p]) for p in params.keys() if
                                          p in [ 'king_format', 'tod', 'time_unit_list',
                                                'mend', 'yend', 'co_code_list',
                                                'name_tatser_list', 'ybegin', 'mbegin',
                                                'code_list', 'co_name_tatser_list', 'level_1',
                                                'level_2', 'level_3']))
            self.browser.open(url+'?'+params_query)
            content = self.browser.response().read()
            #L.info( "%s  xml content length %d" % ('    '*level, len(content)) )
            content = content.replace('iso-8859-8-i','iso-8859-8')
            try:
                doc = ET.fromstring(content)
                for series in doc.xpath('/series_ts/Data_Set/Series'):
                    #print(series.attrib)
                    for elt in series.xpath('obs'):
                        metadata = dict([ (k,v) for k,v in series.attrib.iteritems() ])
                        topics = metadata['name_topic']
                        topics = topics.split(' - ')
                        topics = [ x.strip() for x in topics ]
                        topics.append(metadata['name_ser'].strip())
                        metadata['topics'] = topics
                        metadata['title'] = "%(name_topic)s / %(name_ser)s, %(data_kind)s - %(unit_kind)s per %(time_unit)s" % metadata
                        if 'calc_kind' in metadata.keys():
                            metadata['title'] += " (%(calc_kind)s)" % metadata
                        d = {'metadata': dict([ (k,v) for k,v in series.attrib.iteritems() ])}
                        try:
                            year = int(elt.attrib['time_period'])
                            months = range(1,13)
                            timeslug = "%s" % year
                        except ValueError:
                            try:
                                year, month = [ int(x) for x in elt.attrib['time_period'].split('-') ]
                                months = [month] 
                                timeslug = "%s_%s" % (year, month)
                            except:
                                year, quarter = elt.attrib['time_period'].split('-')
                                year = int(year)
                                assert(quarter[0] == 'Q' )
                                q = int(quarter[1])
                                months = range(q*3+1,q*3+4) 
                                timeslug = "%s_%s" % (year, quarter)
    
                        if elt.attrib['value']:
                            value = float(elt.attrib['value'])
                        else:
                            value = None
                        d['time'] = { 'year' : year, 'months' : months } 
                        d['value'] = value
                        d['slug'] = "%s_%s" % (slug[1:],timeslug)
                        self.data.append(d)
    #                    print json.dumps(d)
                    self.downloaded += 1
            except:
                filename = "out/error/%s" % url
                L.error("FAILED TO PARSE CONTENT here, dumping to %s" % filename)
                f = file(filename,'w')
                f.write(content)
                f.close()
            self.browser.back()
            return

#        for x in self.browser.form.controls:
#            print x, x.attrs
            
        selects = [x for x in self.browser.form.controls if x.type=='select']

        control = selects[-1]

        control_name = control.attrs['name']
        if control_name in ['ybegin','yend','mbegin','mend']:
            self.browser.submit()
            self.parse_form(form_number+1, level+1, slug)
            self.browser.back()
            return
        L.info( "%schanging control %s" % ('    '*level, control_name) )
        for option in control.items:
            if option.name == "0": continue
            L.info( "%s  option %s - %s" % ('    '*level,
                                            option.name,
                                            option.attrs['label']) )
            self.browser.form[control_name] = [option.name]
            self.browser.submit()
            self.parse_form(form_number+1, level+1,slug+"_"+option.attrs['label'].lower().replace(' ','.'))
            self.browser.back()
            self.browser.select_form(nr=form_number)
        return

if __name__ == "__main__":

    try:
        successful_categories = cPickle.load(file('status'))  
    except Exception, e:
        successful_categories = set()

    x = scraper()
       
    for category in range(200):

        if category in successful_categories:
            continue
        try:
            L.exception( ">>>> PARSING CATEGORY %d" % category )
            x.scrape_category(category)
            x.dump('out/output-%d.jsons' % category)
            successful_categories.add(category)
        except KeyboardInterrupt:
            break
        except:
            L.exception( "!!!!!: FAILED TO PARSE CATEGORY %d" % category )
            x.dump('out/error/output-%d.error.jsons' % category)
        finally:
            L.exception( "<<<<< DONE PARSING CATEGORY %d" % category )

    cPickle.dump(successful_categories,file('status','w'))  
