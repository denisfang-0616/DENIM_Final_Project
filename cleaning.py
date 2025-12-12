
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os
import re

load_dotenv()

db_params = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}


GLOBAL_UNDERGRAD_RANKINGS = {

    'harvard': 1,
    'harvard university': 1,
    'massachusetts institute of technology': 1,
    'mit': 1,
    'stanford': 1,
    'stanford university': 1,
    'university of chicago': 1,
    'chicago': 1,
    'uchicago': 1,
    'princeton': 1,
    'princeton university': 1,
    'university of california berkeley': 1,
    'berkeley': 1,
    'uc berkeley': 1,
    'ucb': 1,
    'cal': 1,
    'yale': 1,
    'yale university': 1,
    'london school of economics and political science': 1,
    'lse': 1,
    'london school of economics': 1,
    'university of oxford': 1,
    'oxford': 1,
    'oxford university': 1,
    'university of cambridge': 1,
    'cambridge': 1,
    'cambridge university': 1,

    'columbia': 2,
    'columbia university': 2,
    'northwestern': 2,
    'northwestern university': 2,
    'university of california los angeles': 2,
    'ucla': 2,
    'university of pennsylvania': 2,
    'penn': 2,
    'upenn': 2,
    'wharton': 2,
    'new york university': 2,
    'nyu': 2,
    'nyu stern': 2,
    'stern': 2,
    'national university of singapore': 2,
    'nus': 2,
    'university commerciale luigi bocconi': 2,
    'bocconi': 2,
    'bocconi university': 2,
    'bocconi uni': 2,
    'boccuni': 2,
    'tsinghua': 2,
    'tsinghua university': 2,
    'ucl': 2,
    'university college london': 2,
    'peking': 2,
    'peking university': 2,
    'pku': 2,

    'university of toronto': 3,
    'toronto': 3,
    'utoronto': 3,
    'u of t': 3,
    'uoft': 3,
    'university of michigan ann arbor': 3,
    'university of michigan': 3,
    'michigan': 3,
    'umich': 3,
    'michigan ross': 3,
    'boston': 3,
    'boston university': 3,
    'bu': 3,
    'university of british columbia': 3,
    'ubc': 3,
    'british columbia': 3,
    'duke': 3,
    'duke university': 3,
    'university of california san diego': 3,
    'ucsd': 3,
    'uc san diego': 3,
    'cornell': 3,
    'cornell university': 3,
    'hong kong university of science and technology': 3,
    'hkust': 3,
    'eth zurich swiss federal institute of technology': 3,
    'eth zurich': 3,
    'eth': 3,
    'swiss federal institute of technology': 3,
    'university of tokyo': 3,
    'tokyo': 3,
    'fudan': 3,
    'fudan university': 3,
    'london business school': 3,
    'lbs': 3,
    'university of hong kong': 3,
    'hku': 3,
    'university pompeu fabra': 3,
    'universitat pompeu fabra': 3,
    'upf': 3,
    'pompeu fabra': 3,
    'imperial college london': 3,
    'imperial': 3,
    'imperial college': 3,
    'university of warwick': 3,
    'warwick': 3,
    'chinese university of hong kong': 3,
    'cuhk': 3,
    'seoul national university': 3,
    'seoul national': 3,
    'snu': 3,
    'australian national university': 3,
    'anu': 3,
    'university of new south wales': 3,
    'unsw': 3,
    'shanghai jiao tong university': 3,
    'shanghai jiao tong': 3,
    'sjtu': 3,
    'monash': 3,
    'monash university': 3,
    'nanyang technological university': 3,
    'nanyang': 3,
    'ntu singapore': 3,
    'ntu': 3,
    'brown': 3,
    'brown university': 3,
    'university of melbourne': 3,
    'melbourne': 3,
    'korea university': 3,
    'university of zurich': 3,
    'zurich': 3,
    'uzh': 3,
    'zhejiang': 3,
    'zhejiang university': 3,
    'university of sydney': 3,
    'sydney': 3,
    'yonsei': 3,
    'yonsei university': 3,

    'university of minnesota twin cities': 4,
    'university of minnesota': 4,
    'minnesota': 4,
    'umn': 4,
    'ludwig maximilians universität münchen': 4,
    'lmu munich': 4,
    'lmu': 4,
    'munich': 4,
    'tilburg': 4,
    'tilburg university': 4,
    'erasmus university rotterdam': 4,
    'erasmus': 4,
    'erasmus rotterdam': 4,
    'paris school of economics': 4,
    'pse': 4,
    'university of texas at austin': 4,
    'texas': 4,
    'ut austin': 4,
    'ut-austin': 4,
    'california institute of technology': 4,
    'caltech': 4,
    'stockholm school of economics': 4,
    'sse': 4,
    'stockholm sse': 4,
    'rheinische friedrich wilhelms universität bonn': 4,
    'university of bonn': 4,
    'bonn': 4,
    'bonn university': 4,
    'university of manchester': 4,
    'manchester': 4,
    'university of wisconsin madison': 4,
    'wisconsin': 4,
    'uw madison': 4,
    'uw-madison': 4,
    'wisconsin madison': 4,
    'johns hopkins university': 4,
    'johns hopkins': 4,
    'jhu': 4,
    'university of amsterdam': 4,
    'amsterdam': 4,
    'university of illinois at urbana champaign': 4,
    'illinois': 4,
    'uiuc': 4,
    'illinois urbana-champaign': 4,
    'illinois urbana champaign': 4,
    'singapore management university': 4,
    'smu singapore': 4,
    'universität mannheim': 4,
    'mannheim': 4,
    'mannheim university': 4,
    'carnegie mellon university': 4,
    'carnegie mellon': 4,
    'cmu': 4,
    'cmu tepper': 4,
    'tepper': 4,
    'kaist': 4,
    'université toulouse 1 capitole': 4,
    'toulouse': 4,
    'tse': 4,
    'toulouse school of economics': 4,
    'university of california davis': 4,
    'uc davis': 4,
    'davis': 4,
    'university of queensland': 4,
    'queensland': 4,
    'uq': 4,
    'dartmouth': 4,
    'dartmouth college': 4,
    'ku leuven': 4,
    'leuven': 4,
    'catholic university of leuven': 4,
    'université psl': 4,
    'psl': 4,
    'paris sciences et lettres': 4,
    'universidad carlos iii de madrid': 4,
    'uc3m': 4,
    'carlos iii': 4,
    'universidad carlos iii': 4,
    'kyoto': 4,
    'kyoto university': 4,
    'hec paris': 4,
    'copenhagen business school': 4,
    'cbs': 4,
    'university of edinburgh': 4,
    'edinburgh': 4,
    'city university of hong kong': 4,
    'cityu': 4,
    'michigan state university': 4,
    'michigan state': 4,
    'msu': 4,
    'university of southern california': 4,
    'usc': 4,
    'mcgill': 4,
    'mcgill university': 4,
    'humboldt universität zu berlin': 4,
    'humboldt': 4,
    'humboldt university': 4,
    'national taiwan university': 4,
    'ntu taiwan': 4,
    'universitat autònoma de barcelona': 4,
    'uab': 4,
    'autonomous barcelona': 4,
    'université catholique de louvain': 4,
    'uc louvain': 4,
    'louvain': 4,
    'university of copenhagen': 4,
    'copenhagen': 4,
    'pennsylvania state university': 4,
    'penn state': 4,
    'psu': 4,
    'university of nottingham': 4,
    'nottingham': 4,
    'waseda': 4,
    'waseda university': 4,
    'queen mary university of london': 4,
    'queen mary': 4,
    'qmul': 4,
    'ohio state university': 4,
    'ohio state': 4,
    'osu': 4,
}


PHD_ECON_RANKINGS = {

    'mit': 1, 'massachusetts institute of technology': 1,
    'harvard': 1, 'harvard university': 1,
    'stanford': 1, 'stanford university': 1, 'stanford gsb': 1,
    'princeton': 1, 'princeton university': 1,
    'berkeley': 1, 'uc berkeley': 1, 'ucb': 1, 'cal': 1, 'university of california berkeley': 1,
    'yale': 1, 'yale university': 1,
    'university of chicago': 1, 'uchicago': 1, 'chicago booth': 1, 'booth': 1,
    'northwestern': 1, 'northwestern university': 1, 'northwestern kellogg': 1, 'kellogg': 1,
    'columbia': 1, 'columbia university': 1,
    'nyu': 1, 'new york university': 1, 'nyu stern': 1,
    
    'penn': 2, 'upenn': 2, 'university of pennsylvania': 2, 'wharton': 2, 'upenn wharton': 2,
    'michigan': 2, 'umich': 2, 'university of michigan': 2, 'michigan ross': 2,
    'ucsd': 2, 'uc san diego': 2, 'university of california san diego': 2,
    'ucla': 2, 'university of california los angeles': 2, 'ucla anderson': 2,
    'wisconsin': 2, 'uw madison': 2, 'uw-madison': 2, 'university of wisconsin madison': 2, 'wisconsin madison': 2,
    'minnesota': 2, 'umn': 2, 'university of minnesota': 2,
    'duke': 2, 'duke university': 2,
    'caltech': 2, 'california institute of technology': 2,
    'lse': 2, 'london school of economics': 2, 'london school of economics and political science': 2,
    'oxford': 2, 'university of oxford': 2, 'oxford university': 2, 'nuffield': 2, 'nuffield college': 2,
    
    'cornell': 3, 'cornell university': 3,
    'brown': 3, 'brown university': 3,
    'boston university': 3, 'bu': 3,
    'johns hopkins': 3, 'jhu': 3, 'johns hopkins university': 3,
    'maryland': 3, 'umd': 3, 'university of maryland': 3, 'maryland college park': 3,
    'rochester': 3, 'university of rochester': 3,
    'texas': 3, 'ut austin': 3, 'university of texas austin': 3, 'ut-austin': 3,
    'ohio state': 3, 'osu': 3, 'ohio state university': 3,
    'penn state': 3, 'psu': 3, 'pennsylvania state university': 3,
    'carnegie mellon': 3, 'cmu': 3, 'carnegie mellon university': 3, 'cmu tepper': 3, 'tepper': 3,
    'usc': 3, 'university of southern california': 3, 'usc price': 3,
    'georgia tech': 3, 'georgia institute of technology': 3, 'georgiatech': 3, 'gt': 3,
    'arizona': 3, 'university of arizona': 3,
    'vanderbilt': 3, 'vanderbilt university': 3,
    'georgetown': 3, 'georgetown university': 3,
    'emory': 3, 'emory university': 3,
    'virginia': 3, 'uva': 3, 'university of virginia': 3,
    'unc': 3, 'university of north carolina': 3, 'unc chapel hill': 3, 'unc-ch': 3, 'north carolina chapel hill': 3,
    'washington university': 3, 'wustl': 3, 'washu': 3, 'wash u': 3, 'washington university in st louis': 3, 'washington university st louis': 3,
    'rice': 3, 'rice university': 3,
    'notre dame': 3, 'university of notre dame': 3,
    'texas a&m': 3, 'tamu': 3, 'texas a&m university': 3,
    'indiana': 3, 'iu': 3, 'indiana university': 3, 'indiana bloomington': 3,
    'michigan state': 3, 'msu': 3, 'michigan state university': 3,
    'pittsburgh': 3, 'pitt': 3, 'university of pittsburgh': 3,
    'ucsb': 3, 'uc santa barbara': 3, 'university of california santa barbara': 3, 'santa barbara': 3,
    'uc irvine': 3, 'uci': 3, 'irvine': 3, 'university of california irvine': 3,
    'uc davis': 3, 'davis': 3, 'university of california davis': 3,
    'illinois': 3, 'uiuc': 3, 'university of illinois': 3, 'illinois urbana-champaign': 3, 'illinois urbana champaign': 3,
    'washington': 3, 'uw': 3, 'university of washington': 3, 'uw seattle': 3,
    'rutgers': 3, 'rutgers university': 3,
    'boston college': 3, 'bc': 3,
    'brandeis': 3, 'brandeis university': 3,
    'tufts': 3, 'tufts university': 3,
    
    'cambridge': 3, 'university of cambridge': 3, 'cambridge university': 3,
    'ucl': 3, 'university college london': 3,
    'warwick': 3, 'warwick university': 3, 'university of warwick': 3,
    'bocconi': 3, 'bocconi university': 3, 'bocconi uni': 3, 'boccuni': 3,
    'tilburg': 3, 'tilburg university': 3,
    'upf': 3, 'pompeu fabra': 3, 'universitat pompeu fabra': 3,
    'barcelona gse': 3, 'bgse': 3, 'barcelona graduate school of economics': 3, 'barcelona graduate school': 3, 'barcelona school of economics': 3,
    'tse': 3, 'toulouse': 3, 'toulouse school of economics': 3,
    'stockholm school of economics': 3, 'sse': 3, 'stockholm sse': 3,
    'mannheim': 3, 'mannheim university': 3, 'university of mannheim': 3,
    'bonn': 3, 'bonn university': 3, 'university of bonn': 3, 'bonn graduate school': 3,
    'zurich': 3, 'university of zurich': 3, 'uzh': 3,
    'erasmus': 3, 'erasmus university': 3, 'erasmus rotterdam': 3, 'erasmus university rotterdam': 3,
    'tinbergen': 3, 'tinbergen institute': 3,
    'uc3m': 3, 'carlos iii': 3, 'universidad carlos iii': 3,
    'cemfi': 3, 'cemfi madrid': 3,
    'sciences po': 3, 'sciences po paris': 3,
    'paris school of economics': 3, 'pse': 3,
    'maastricht': 3, 'maastricht university': 3,
    'frankfurt': 3, 'goethe university frankfurt': 3, 'gsefm': 3, 'frankfurt gsefm': 3,
    
    'toronto': 3, 'university of toronto': 3, 'utoronto': 3, 'u of t': 3, 'uoft': 3,
    'ubc': 3, 'british columbia': 3, 'university of british columbia': 3,
    'mcgill': 3, 'mcgill university': 3,
    'western': 3, 'western ontario': 3, 'western university': 3, 'university of western ontario': 3, 'uwo': 3,
    'queens': 3, "queen's": 3, "queen's university": 3, 'queens university': 3,
    
    
    'university of illinois chicago': 4, 'illinois chicago': 4, 'uic': 4, 
    'university of illinois at chicago': 4, 'ui chicago': 4,
    
    'asu': 4, 'arizona state': 4, 'arizona state university': 4,
    'florida': 4, 'uf': 4, 'university of florida': 4,
    'purdue': 4, 'purdue university': 4,
    'colorado': 4, 'cu boulder': 4, 'university of colorado': 4, 'colorado boulder': 4, 'cu-boulder': 4,
    'north carolina state': 4, 'nc state': 4, 'ncsu': 4,
    'iowa state': 4, 'iowa state university': 4,
    'kansas': 4, 'university of kansas': 4,
    'virginia tech': 4, 'vt': 4, 'virginia polytechnic': 4,
    'oregon': 4, 'university of oregon': 4, 'uo': 4,
    'oregon state': 4, 'oregon state university': 4,
    'utah': 4, 'university of utah': 4,
    'connecticut': 4, 'uconn': 4, 'university of connecticut': 4,
    'delaware': 4, 'university of delaware': 4,
    'massachusetts': 4, 'umass': 4, 'umass amherst': 4, 'university of massachusetts': 4,
    'stony brook': 4, 'stony brook university': 4, 'suny stony brook': 4,
    'suny buffalo': 4, 'buffalo': 4, 'university at buffalo': 4,
    'lehigh': 4, 'lehigh university': 4,
    'clark': 4, 'clark university': 4,
    'temple': 4, 'temple university': 4,
    'american': 4, 'american university': 4,
    'george washington': 4, 'gwu': 4, 'george washington university': 4, 'gw': 4,
    'george mason': 4, 'gmu': 4, 'george mason university': 4,
    'cuny': 4, 'cuny graduate center': 4, 'city university of new york': 4,
    'fordham': 4, 'fordham university': 4,
    'southern methodist': 4, 'smu': 4, 'southern methodist university': 4,
    'tulane': 4, 'tulane university': 4,
    'miami': 4, 'university of miami': 4,
    'alabama': 4, 'university of alabama': 4,
    'georgia': 4, 'uga': 4, 'university of georgia': 4,
    'kentucky': 4, 'university of kentucky': 4, 'uk': 4,
    'south carolina': 4, 'university of south carolina': 4,
    'tennessee': 4, 'university of tennessee': 4,
    'missouri': 4, 'university of missouri': 4,
    'nebraska': 4, 'university of nebraska': 4,
    'oklahoma': 4, 'university of oklahoma': 4,
    'houston': 4, 'university of houston': 4,
    'binghamton': 4, 'binghamton university': 4, 'suny binghamton': 4,
    'syracuse': 4, 'syracuse university': 4,
    'drexel': 4, 'drexel university': 4,
    'wayne state': 4, 'wayne state university': 4,
    'iowa': 4, 'university of iowa': 4,
    'cincinnati': 4, 'university of cincinnati': 4,
    'clemson': 4, 'clemson university': 4,
    'florida state': 4, 'fsu': 4, 'florida state university': 4,
    'wyoming': 4, 'university of wyoming': 4,
    'arkansas': 4, 'university of arkansas': 4,
    'new mexico': 4, 'unm': 4, 'university of new mexico': 4,
    'riverside': 4, 'uc riverside': 4, 'ucr': 4, 'university of california riverside': 4,
    'santa cruz': 4, 'ucsc': 4, 'uc santa cruz': 4, 'university of california santa cruz': 4,
    'washington state': 4, 'wsu': 4, 'washington state university': 4,
    
    'mcmaster': 4, 'mcmaster university': 4,
    'simon fraser': 4, 'sfu': 4, 'simon fraser university': 4,
    'waterloo': 4, 'university of waterloo': 4, 'uwaterloo': 4,
    'calgary': 4, 'university of calgary': 4, 'ucalgary': 4,
    'alberta': 4, 'university of alberta': 4, 'ualberta': 4,
    'victoria': 4, 'university of victoria': 4, 'uvic': 4,
    'carleton': 4, 'carleton university': 4,
    'ottawa': 4, 'university of ottawa': 4, 'uottawa': 4,
    'york': 4, 'york university': 4,
    'concordia': 4, 'concordia university': 4,
    'wilfrid laurier': 4, 'laurier': 4, 'wlu': 4,
    'dalhousie': 4, 'dalhousie university': 4, 'dal': 4,
    'laval': 4, 'laval university': 4, 'université laval': 4, 'universite laval': 4,
    'montreal': 4, 'université de montréal': 4, 'universite de montreal': 4, 'umontreal': 4,
    
    'imperial': 4, 'imperial college': 4, 'imperial college london': 4,
    'edinburgh': 4, 'university of edinburgh': 4,
    'bristol': 4, 'university of bristol': 4,
    'essex': 4, 'university of essex': 4,
    'nottingham': 4, 'university of nottingham': 4,
    'manchester': 4, 'university of manchester': 4,
    'queen mary': 4, 'qmul': 4, 'queen mary university': 4, 'queen mary university london': 4,
    'kings': 4, "king's college": 4, "king's college london": 4, 'kcl': 4,
    'sussex': 4, 'university of sussex': 4,
    'lbs': 4, 'london business school': 4,
    'insead': 4,
    'eui': 4, 'european university institute': 4,
    'aalto': 4, 'aalto university': 4, 'aalto school of business': 4,
    'ceu': 4, 'central european university': 4,
    'copenhagen': 4, 'university of copenhagen': 4,
    'aarhus': 4, 'aarhus university': 4, 'aarhus bss': 4,
    'vienna': 4, 'university of vienna': 4, 'wu wien': 4,
    'stockholm university': 4,
    'uppsala': 4, 'uppsala university': 4,
    'helsinki': 4, 'university of helsinki': 4,
    'amsterdam': 4, 'university of amsterdam': 4,
    'vrije universiteit': 4, 'vu amsterdam': 4,
    'groningen': 4, 'university of groningen': 4,
    'utrecht': 4, 'utrecht university': 4,
    'leiden': 4, 'leiden university': 4,
    'geneva': 4, 'university of geneva': 4,
    'lausanne': 4, 'university of lausanne': 4,
    'basel': 4, 'university of basel': 4,
    'bern': 4, 'university of bern': 4,
    'munich': 4, 'lmu munich': 4, 'ludwig maximilian university': 4,
    'humboldt': 4, 'humboldt university': 4,
    'berlin': 4, 'tu berlin': 4, 'technical university of berlin': 4,
    'cologne': 4, 'university of cologne': 4,
    'heidelberg': 4, 'heidelberg university': 4,
    'saarland': 4, 'saarland university': 4,
    'bologna': 4, 'university of bologna': 4,
    'louvain': 4, 'ku leuven': 4, 'catholic university of leuven': 4, 'uc louvain': 4,
    'ghent': 4, 'ghent university': 4,
    'paris': 4, 'sorbonne': 4, 'université paris': 4, 'universite paris': 4, 'paris 1': 4, 'pantheon sorbonne': 4,
    'qem': 4,
    'nova': 4, 'nova sbe': 4, 'nova school of business': 4,
    'norwegian school of economics': 4, 'nhh': 4,
    
    'nus': 4, 'national university of singapore': 4,
    'smu singapore': 4, 'singapore management university': 4,
    'hku': 4, 'university of hong kong': 4,
    'hkust': 4, 'hong kong university of science and technology': 4,
    'cuhk': 4, 'chinese university of hong kong': 4,
    'tokyo': 4, 'university of tokyo': 4,
    'kyoto': 4, 'kyoto university': 4,
    'seoul': 4, 'seoul national university': 4, 'snu': 4,
    'yonsei': 4, 'yonsei university': 4,
    'anu': 4, 'australian national university': 4,
    'melbourne': 4, 'university of melbourne': 4,
    'sydney': 4, 'university of sydney': 4,
    'unsw': 4, 'university of new south wales': 4,
    'monash': 4, 'monash university': 4,
}
def normalize_name(name):
    if pd.isna(name) or not name:
        return ''
    name = str(name).lower().strip()
    
    name = name.replace('université', 'university')
    name = name.replace('universite', 'university')
    name = name.replace('universität', 'university')
    name = name.replace('universitat', 'university')
    name = name.replace('univercity', 'university')
    name = name.replace('univesity', 'university')
    
    name = re.sub(r'\s+(university|college|institute|school|uni|univ)$', '', name)
    name = re.sub(r'^the\s+', '', name)
    name = re.sub(r'[,\.\-\']', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name


def match_university(name, all_rankings):
    """Try to match university to rankings with flexible matching"""
    if pd.isna(name) or not name:
        return None
    
    normalized = normalize_name(name)
    
    if normalized in all_rankings:
        return all_rankings[normalized]
    
    if len(normalized) <= 2:
        return None
    
    stop_words = {'of', 'the', 'at', 'in', 'and', 'for'}
    ambiguous_words = {'chicago', 'illinois', 'columbia', 'washington', 'texas'}
    
    normalized_words = set(normalized.split()) - stop_words
    
    for key, rank in all_rankings.items():
        key_words = set(key.split()) - stop_words
        
        if len(key) <= 2:
            continue
        
        if len(key_words) == 0:
            continue
        
        if len(key_words) == 1:
            key_word = list(key_words)[0]
            
            if key_word in ambiguous_words:
                if len(normalized_words) == 1 and key_word in normalized_words:
                    return rank
                continue
            
            if key_word in normalized_words:
                return rank
                
        elif len(key_words) > 1:
            if key_words.issubset(normalized_words):
                return rank
    
    return None


def extract_rank_from_text(text):
    if pd.isna(text) or not text:
        return None
    
    text_lower = str(text).lower()
    
    if 'ivy' in text_lower and 'public' not in text_lower:
        return 2
    
    if any(p in text_lower for p in ['top 5', 'top-5', '#1', '#2', '#3', '#4', '#5']):
        return 1
    if any(p in text_lower for p in ['top 10', 'top-10', 't10 ', 'top10']):
        return 2
    if any(p in text_lower for p in ['top 15', 'top-15', 'top 20', 'top-20', 't15 ', 't20 ', 'top15', 'top20']):
        return 3
    if any(p in text_lower for p in ['top 25', 'top-25', 'top 30', 'top-30', 't25 ', 't30 ', 'top25', 'top30', 'top 35', 'top-35', 'top 40', 'top-40', 't40 ', 'top40']):
        return 4
    if any(p in text_lower for p in ['top 50', 'top-50', 't50 ', 'top50', 'top 75', 'top-75', 'top 100', 'top-100', 't100 ', 'top100']):
        return 5
    
    if any(p in text_lower for p in ['tier 1', 'tier-1', 'tier1', 't1 ', ' t1', 't-1', '1st tier', 'first tier', '1st-tier', 'tier i ']):
        return 2
    if any(p in text_lower for p in ['tier 2', 'tier-2', 'tier2', 't2 ', ' t2', 't-2', '2nd tier', 'second tier', '2nd-tier', 'tier ii ']):
        return 3
    if any(p in text_lower for p in ['tier 3', 'tier-3', 'tier3', 't3 ', ' t3', 't-3', '3rd tier', 'third tier', '3rd-tier', 'tier iii ']):
        return 4
    if any(p in text_lower for p in ['tier 4', 'tier-4', 'tier4', 't4 ', ' t4', 't-4', '4th tier', 'fourth tier', '4th-tier']):
        return 5
    
    if 'flagship' in text_lower:
        return 5
    if any(p in text_lower for p in ['big 10', 'big ten', 'big10', 'b10 ', 'b1g', 'big-10']):
        return 4
    if 'public ivy' in text_lower or 'public-ivy' in text_lower:
        return 4
    
    if any(p in text_lower for p in ['t10 lac', 'top 10 lac', 'top-10 lac', 'top10 lac']):
        return 4
    if any(p in text_lower for p in ['t20 lac', 'top 20 lac', 'top-20 lac', 't15 lac', 'top15 lac', 'top20 lac']):
        return 5
    
    if 'r1' in text_lower or 'r-1' in text_lower or ' r1 ' in text_lower:
        return 5
    
    if any(p in text_lower for p in ['canadian top 3', 'top 3 canadian', 'canada top 3']):
        return 3
    if any(p in text_lower for p in ['canadian top 5', 'top 5 canadian', 'top canadian']):
        return 4
    if 't3 canadian' in text_lower or 't2 canadian' in text_lower:
        return 4
    
    if any(p in text_lower for p in ['best university', 'best in', '#1 in', 'leading university']):
        return 3
    
    if any(p in text_lower for p in ['top public', 'top state', 'best public']):
        return 4
    
    if any(p in text_lower for p in ['elite', 'prestigious', 'reputable', 'well-known']):
        return 5
    
    return None


def rank_university_undergrad(name):
    rank = match_university(name, GLOBAL_UNDERGRAD_RANKINGS)
    if rank is None:
        rank = extract_rank_from_text(name)
    return rank


def rank_university_phd(name):
    rank = match_university(name, PHD_ECON_RANKINGS)
    return rank


def rank_undergrad_institution(institution):
    if pd.isna(institution) or str(institution).strip() == '':
        return None
    rank = rank_university_undergrad(institution)
    return rank if rank else 5 


def rank_phd_schools(schools):
    if schools is None:
        return None
    
    if isinstance(schools, (list, np.ndarray)):
        if len(schools) == 0:
            return None
        school_list = schools
    else:
        if pd.isna(schools):
            return None
        school_str = str(schools).strip()
        if not school_str:
            return None
        school_list = [school_str]
    
    ranks = []
    for school in school_list:
        if school is None:
            continue
        if isinstance(school, float) and pd.isna(school):
            continue
        if not str(school).strip():
            continue
        rank = rank_university_phd(school) 
        if rank:
            ranks.append(rank)
    
    return min(ranks) if ranks else None


def standardize_gpa(gpa, gpa_out_of):
    if pd.isna(gpa) or pd.isna(gpa_out_of):
        return None
    if gpa_out_of == 0 or gpa_out_of is None:
        return None
    try:
        gpa = float(gpa)
        gpa_out_of = float(gpa_out_of)
        return round((gpa / gpa_out_of) * 4.0, 2)
    except (ValueError, TypeError, ZeroDivisionError):
        return None


def has_grad_program(row):
    has_grad_gpa = pd.notna(row['grad_gpa'])
    has_grad_inst = pd.notna(row['grad_institution']) and str(row['grad_institution']).strip() != ''
    return 1 if (has_grad_gpa or has_grad_inst) else 0


def has_calculus(math_courses):
    if math_courses is None:
        return 0
    
    if isinstance(math_courses, (list, np.ndarray)):
        if len(math_courses) == 0:
            return 0
        courses_str = ' '.join(str(c).lower() for c in math_courses)
    else:
        if pd.isna(math_courses):
            return 0
        courses_str = str(math_courses).lower()
    
    patterns = ['calc', 'calculus', 'ap', 'bc']
    return 1 if any(p in courses_str for p in patterns) else 0


def has_linear_algebra(math_courses):
    if math_courses is None:
        return 0
    
    if isinstance(math_courses, (list, np.ndarray)):
        if len(math_courses) == 0:
            return 0
        courses_str = ' '.join(str(c).lower() for c in math_courses)
    else:
        if pd.isna(math_courses):
            return 0
        courses_str = str(math_courses).lower()
    
    patterns = ['linear', 'lin ', 'matrix', 'vector', 'matrices']
    return 1 if any(p in courses_str for p in patterns) else 0


def has_real_analysis(math_courses):
    if math_courses is None:
        return 0
    
    if isinstance(math_courses, (list, np.ndarray)):
        if len(math_courses) == 0:
            return 0
        courses_str = ' '.join(str(c).lower() for c in math_courses)
    else:
        if pd.isna(math_courses):
            return 0
        courses_str = str(math_courses).lower()
    
    patterns = [
        'real', 'mathematical analysis', 'metric spaces', 'advanced analytic',
        'grad level analysis', 'intro to proofs', 'analysis', ' ra ', ' ra,',
        'analysis 1', 'analysis 2', 'analysis 3', 'analysis 4',
        'analysis i', 'analysis ii', 'analysis iii', 'analysis iv', 'analytic'
    ]
    
    return 1 if any(p in courses_str for p in patterns) else 0
def convert_old_gre_to_new(score, is_quant=True):
    score = int(score)
    
    if 130 <= score <= 170:
        return score

    if is_quant:
        if score >= 800:
            return 170
        elif score >= 760:
            return 169
        elif score >= 740:
            return 168
        elif score >= 720:
            return 167
        elif score >= 700:
            return 166
        elif score >= 680:
            return 165
        elif score >= 660:
            return 164
        elif score >= 640:
            return 163
        elif score >= 620:
            return 162
        elif score >= 600:
            return 161
        elif score >= 580:
            return 160
        elif score >= 560:
            return 159
        elif score >= 540:
            return 158
        elif score >= 520:
            return 157
        elif score >= 500:
            return 156
        elif score >= 480:
            return 155
        elif score >= 460:
            return 154
        elif score >= 440:
            return 153
        elif score >= 420:
            return 152
        elif score >= 400:
            return 151
        else:
            return 150
    else:
        if score >= 800:
            return 170
        elif score >= 730:
            return 169
        elif score >= 700:
            return 168
        elif score >= 670:
            return 167
        elif score >= 640:
            return 166
        elif score >= 610:
            return 165
        elif score >= 580:
            return 164
        elif score >= 550:
            return 163
        elif score >= 520:
            return 162
        elif score >= 500:
            return 161
        elif score >= 470:
            return 160
        elif score >= 450:
            return 159
        elif score >= 430:
            return 158
        elif score >= 410:
            return 157
        elif score >= 390:
            return 156
        elif score >= 370:
            return 155
        elif score >= 350:
            return 154
        else:
            return 153


def process_gre_gmat(row):
    quant = row['gre_quant']
    verbal = row['gre_verbal']
    writing = row['gre_writing']
    
    result = {
        'gre_quant_std': None,
        'gre_verbal_std': None,
        'gmat_quant': None,
        'gmat_verbal': None,
        'gmat_writing': None
    }
    
    if pd.notna(quant) and quant != 0:
        quant = int(quant)
        if 6 <= quant <= 51:
            result['gmat_quant'] = quant
            if pd.notna(writing):
                result['gmat_writing'] = writing
        elif 130 <= quant <= 200 or 200 <= quant <= 800:
            result['gre_quant_std'] = convert_old_gre_to_new(quant, is_quant=True)
    
    if pd.notna(verbal) and verbal != 0:
        verbal = int(verbal)
        if 6 <= verbal <= 51:
            result['gmat_verbal'] = verbal
        elif 130 <= verbal <= 200 or 200 <= verbal <= 800:
            result['gre_verbal_std'] = convert_old_gre_to_new(verbal, is_quant=False)
    
    return pd.Series(result)


def is_econ_related(major):
    if pd.isna(major) or str(major).strip() == '':
        return 0
    
    major_lower = str(major).lower()
    
    patterns = [
        'economics', 'accounting', 'finance', 'actuarial', 'business',
        'econ', 'eco', 'b.b.a', 'bba', 'commerce', 'management'
    ]
    
    return 1 if any(p in major_lower for p in patterns) else 0


def categorize_lor(lor_text):
    if pd.isna(lor_text) or str(lor_text).strip() == '':
        return {'academic_lor': 0, 'research_lor': 0, 'professional_lor': 0}
    
    text = str(lor_text).lower()
    
    academic_patterns = [
        'professor', 'prof ', ' prof,', 'prof.', 'associate prof', 'assistant prof',
        'lecturer', 'instructor', 'dean', 'chair', 'hod', 'director',
        'advisor', 'adviser', 'thesis advisor', 'supervisor',
        'phd', 'dphil', 'postdoc',
        'course', 'class', 'undergraduate', 'graduate', 'masters',
        'university', 'college', 'alma mater'
        ' ra ', 'research assistant', 'research supervisor', 'research advisor', 'research prof',
        'thesis', 'dissertation',
        'fed', 'federal reserve',
        'think tank', 'research institute', 'imf', 'oecd', 'ecb',
        'co-author', 'co-write',
        'pre-doc', 'predoc', 'pre-doctoral'
    ]
    
    professional_patterns = [
        'boss', 'supervisor', 'manager', 'director',
        'ceo', 'cfo', 'vp', 'partner', 'cbo', 'cso',
        'employer', 'work', 'company', 'firm', 'industry', 'client',
        'government', 'agency', 'military',
        'medical', 'law', 'engineering', 'cs',
        'non-academic', 'professional'
    ]
    
    academic_score = sum(1 for p in academic_patterns if p in text)
    professional_score = sum(1 for p in professional_patterns if p in text)
    
    if academic_score == 0 and professional_score == 0:
        return {'academic_lor': 0, 'research_lor': 0, 'professional_lor': 1}
    
    result = {'academic_lor': 0, 'research_lor': 0, 'professional_lor': 0}
    
    if academic_score > 0:
        result['academic_lor'] = 1
    elif professional_score > 0:
        result['professional_lor'] = 1
    
    return result


def determine_phd_offer(row):
    accepted = row['schools_accepted']
    applied = row['schools_applied']
    waitlisted = row['schools_waitlisted']
    rejected = row['schools_rejected']
    
    def has_content(field):
        if field is None:
            return False
        if isinstance(field, (list, np.ndarray)):
            return len(field) > 0
        if pd.isna(field):
            return False
        return bool(str(field).strip())
    
    has_accepted = has_content(accepted)
    has_applied = has_content(applied)
    has_waitlisted = has_content(waitlisted)
    has_rejected = has_content(rejected)
    
    if has_accepted:
        return 1
    elif has_applied or has_waitlisted or has_rejected:
        return 0
    else:
        return None


def main():
    conn = psycopg2.connect(**db_params)
    df = pd.read_sql("SELECT * FROM admissions_data", conn)
    
    df['undergrad_gpa_std'] = df.apply(lambda row: standardize_gpa(row['undergrad_gpa'], row['undergrad_gpa_out_of']), axis=1)
    df['grad_gpa_std'] = df.apply(lambda row: standardize_gpa(row['grad_gpa'], row['grad_gpa_out_of']), axis=1)
    df['attended_grad_program'] = df.apply(has_grad_program, axis=1)
    
    df['taken_calculus'] = df['math_courses'].apply(has_calculus)
    df['taken_linear_algebra'] = df['math_courses'].apply(has_linear_algebra)
    df['taken_real_analysis'] = df['math_courses'].apply(has_real_analysis)
    
    gre_gmat_cols = df.apply(process_gre_gmat, axis=1)
    df = pd.concat([df, gre_gmat_cols], axis=1)
    
    df['undergrad_econ_related'] = df['undergrad_major'].apply(is_econ_related)
    
    lor_categories = df['letters_of_rec'].apply(categorize_lor)
    df['academic_lor'] = lor_categories.apply(lambda x: x['academic_lor'])
    df['research_lor'] = lor_categories.apply(lambda x: x['research_lor'])
    df['professional_lor'] = lor_categories.apply(lambda x: x['professional_lor'])
    
    df['got_phd_offer'] = df.apply(determine_phd_offer, axis=1)
    df['phd_course_taken'] = df['phd_course_taken'].apply(lambda x: 1 if x == True else (0 if x == False else None))
    df['research_experience'] = df['research_experience'].apply(lambda x: 1 if x == True else (0 if x == False else None))
    
    df['undergrad_rank'] = df['undergrad_institution'].apply(rank_undergrad_institution)
    df['phd_accepted_rank'] = df.apply(lambda row: rank_phd_schools(row['schools_accepted']), axis=1)
    
    cursor = conn.cursor()
    
    try:
        cursor.execute("DROP TABLE IF EXISTS admissions_data_cleaned CASCADE;")
        conn.commit()
    except Exception:
        conn.rollback()
    
    cursor.execute("""
        CREATE TABLE admissions_data_cleaned AS
        SELECT * FROM admissions_data WHERE 1=0;
    """)
    conn.commit()
    
    cursor.execute("""
        ALTER TABLE admissions_data_cleaned
        ADD COLUMN undergrad_gpa_std DECIMAL(3,2),
        ADD COLUMN grad_gpa_std DECIMAL(3,2),
        ADD COLUMN attended_grad_program INTEGER,
        ADD COLUMN taken_calculus INTEGER,
        ADD COLUMN taken_linear_algebra INTEGER,
        ADD COLUMN taken_real_analysis INTEGER,
        ADD COLUMN gre_quant_std INTEGER,
        ADD COLUMN gre_verbal_std INTEGER,
        ADD COLUMN gmat_quant INTEGER,
        ADD COLUMN gmat_verbal INTEGER,
        ADD COLUMN gmat_writing DECIMAL(3,1),
        ADD COLUMN undergrad_econ_related INTEGER,
        ADD COLUMN academic_lor INTEGER,
        ADD COLUMN research_lor INTEGER,
        ADD COLUMN professional_lor INTEGER,
        ADD COLUMN got_phd_offer INTEGER,
        ADD COLUMN undergrad_rank INTEGER,
        ADD COLUMN phd_accepted_rank INTEGER;
    """)
    conn.commit()
    
    df['phd_course_taken'] = df['phd_course_taken'].apply(lambda x: None if pd.isna(x) else bool(x))
    df['research_experience'] = df['research_experience'].apply(lambda x: None if pd.isna(x) else bool(x))
    
    cols = df.columns.tolist()
    cols_str = ', '.join([f'"{col}"' for col in cols])
    placeholders = ', '.join(['%s'] * len(cols))
    
    insert_query = f'INSERT INTO admissions_data_cleaned ({cols_str}) VALUES ({placeholders})'
    
    data = []
    for _, row in df.iterrows():
        row_data = []
        for val in row:
            if isinstance(val, (list, np.ndarray)):
                row_data.append(list(val))
            elif val is None or (isinstance(val, float) and np.isnan(val)):
                row_data.append(None)
            else:
                row_data.append(val)
        data.append(tuple(row_data))
    
    execute_batch(cursor, insert_query, data, page_size=1000)
    conn.commit()
    
    csv_filename = "admissions_data_cleaned.csv"
    df.to_csv(csv_filename, index=False)
   
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()