"""Export canonical OpenAPI, JSON schemas and dependency-free TS schema types."""
import json
from pathlib import Path
from transitionbench.api import create_app
from transitionbench import schemas

ROOT=Path(__file__).resolve().parents[1]
api=create_app(ROOT/'work'/'schema-export').openapi()
(ROOT/'docs'/'openapi.json').write_text(json.dumps(api,indent=2),encoding='utf-8')
names=['EndpointSpec','Capabilities','WorkloadSpec','ExperimentSpec','SLOSpec','ResourceBudget','WarmupSpec','RequestEvent','WorkerSnapshot','TransitionEvent','DecisionRecord','RunManifest','ValidationReport']
for name in names:
    directory=ROOT/'docs'/'schemas';directory.mkdir(exist_ok=True)
    (directory/(name+'.json')).write_text(json.dumps(getattr(schemas,name).model_json_schema(),indent=2),encoding='utf-8')
from transitionbench.calibration import CalibrationStudy
from transitionbench.preparation import StudyPreparation
from transitionbench.collection import CollectionSpec
from transitionbench.proposals import ProposalInput, OutcomeInput, AutoReviewInput, ImportedEvidence
for record in (CalibrationStudy,StudyPreparation,CollectionSpec,ProposalInput,OutcomeInput,AutoReviewInput,ImportedEvidence):
    (directory/(record.__name__+'.json')).write_text(json.dumps(record.model_json_schema(),indent=2),encoding='utf-8')

def ts(node):
    if '$ref' in node:return node['$ref'].split('/')[-1]
    if 'const' in node:return json.dumps(node['const'])
    if 'enum' in node:return ' | '.join(json.dumps(v) for v in node['enum'])
    if 'anyOf' in node:return ' | '.join(ts(n) for n in node['anyOf'])
    kind=node.get('type')
    if kind=='array':return '('+ts(node.get('items',{}))+')[]'
    if kind=='object':
        props=node.get('properties')
        if props is not None:return '{ '+ '; '.join(json.dumps(k)+('' if k in node.get('required',[]) else '?')+': '+ts(v) for k,v in props.items())+' }'
        extra=node.get('additionalProperties',{})
        return 'Record<string, '+(ts(extra) if isinstance(extra,dict) else 'unknown')+'>'
    return {'string':'string','integer':'number','number':'number','boolean':'boolean','null':'null'}.get(kind,'unknown')
text='// Generated from the canonical Python OpenAPI. Do not edit manually.\n'
text+='\n'.join('export type '+name+' = '+ts(schema)+';' for name,schema in api['components']['schemas'].items())+'\n'
(ROOT/'sdk'/'src'/'schemas.ts').write_text(text,encoding='utf-8')
print('Exported canonical OpenAPI, schemas and TypeScript definitions')
