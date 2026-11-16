import json,sys,collections
s=json.load(open(sys.argv[1])); v=json.load(open(sys.argv[2])); s['slow']=s['slow'] or []
print('acked',s['acked'],'errors',s['errors'],'win',s['errorWindows'],'slow',len(s['slow']),s['slow'][-8:])
print('errkinds',collections.Counter(e[13:75] for e in (s['lastErrors'] or [])))
print('checked',v['checked'],'missing',v['missing'],'wrong',v['wrong'],'readErr',v['readErrors'],'bySec',v['missingBySecond'],'dbsize',v['dbsize'], 'ex', v.get('examples'))
