from rdflib import Bag, Graph, URIRef, Literal, BNode, Seq
from rdflib.namespace import OWL,RDF
from pprint import pprint
import sys
import os
import csv
import pandas as pd
from itertools import zip_longest
import math
import json
from datasketch import MinHashLSHEnsemble, MinHash
import time
from datetime import datetime
from rdflib import Namespace
import re
import owlrl


DEFAULT_DIMENSIONS_DIR="dimensions/Geo_Continent/"
DEFAULT_DIMENSIONS_DIRR="dimensions/"
dl_dim= Namespace("http://w3id.org/kpionto/Dimension#")
dl_lvl= Namespace("http://w3id.org/kpionto/Level#")
dl_prop= Namespace("http://w3id.org/kpionto/#")
dl_member= Namespace("http://w3id.org/kpionto/Member#")






                    
def import_continent(kg):

    with open(DEFAULT_DIMENSIONS_DIR+"Geo.continent","r",encoding="utf-8") as f:
        values = f.read().split("\n") 
        for c in values:
            if(' ' or ',' or '"') in c:
                    c=re.sub('[^0-9a-zA-Z]', '_', c)
            sub=URIRef("http://w3id.org/kpionto/Member#"+c)
            kg.add((sub,RDF.type,dl_member.member))
            kg.add((sub,dl_prop.inLevel,dl_lvl.continent))
    
    
        
                    
def import_country(kg):
        with open(DEFAULT_DIMENSIONS_DIR+"Geo.country","r",encoding="utf-8") as f: 
            values = f.read().split("\n")
            for c in values:
                if(' ' or ',' or '"') in c:
                    c=re.sub('[^0-9a-zA-Z]', '_', c)
                sub=URIRef("http://w3id.org/kpionto/Member#"+c)
                if(c):
                    kg.add((sub,RDF.type,dl_member.member))
                    kg.add((sub,dl_prop.inLevel,dl_lvl.country))
        




def import_country_in_africa(kg):
    with open(DEFAULT_DIMENSIONS_DIR+"Geo.continent.Africa","r",encoding="utf-8") as f: 
            values = f.read().split("\n")
            for c in values:
                if(' ' or ',' or '"') in c:
                    c=re.sub('[^0-9a-zA-Z]', '_', c)
                sub=URIRef("http://w3id.org/kpionto/Member#"+c)
                if(c):
                    kg.add((sub,dl_prop.rollup,dl_member.Africa))
    



def import_country_in_america(kg):
    with open(DEFAULT_DIMENSIONS_DIR+"Geo.continent.America","r",encoding="utf-8") as f: 
            values = f.read().split("\n")
            for c in values:
                if(' ' or ',' or '"') in c:
                    c=re.sub('[^0-9a-zA-Z]', '_', c)
                sub=URIRef("http://w3id.org/kpionto/Member#"+c)
                if(c):
                    kg.add((sub,dl_prop.rollup,dl_member.America))
    



def import_country_in_europe(kg):
    with open(DEFAULT_DIMENSIONS_DIR+"Geo.continent.Europe","r",encoding="utf-8") as f: 
            values = f.read().split("\n")
            for c in values:
                if(' ' or ',' or '"') in c:
                    c=re.sub('[^0-9a-zA-Z]', '_', c)
                sub=URIRef("http://w3id.org/kpionto/Member#"+c)
                if(c):
                    kg.add((sub,dl_prop.rollup,dl_member.Europe))
    






def import_country_in_asia(kg):
    with open(DEFAULT_DIMENSIONS_DIR+"Geo.continent.Asia","r",encoding="utf-8") as f: 
            values = f.read().split("\n")
            for c in values:
                if(' ' or ',' or '"') in c:
                    c=re.sub('[^0-9a-zA-Z]', '_', c)
                sub=URIRef("http://w3id.org/kpionto/Member#"+c)
                if(c):
                    kg.add((sub,dl_prop.rollup,dl_member.Asia))
    




def import_country_in_oceania(kg):
    with open(DEFAULT_DIMENSIONS_DIR+"Geo.continent.Oceania","r",encoding="utf-8") as f: 
            values = f.read().split("\n")
            for c in values:
                if(' ' or ',' or '"') in c:
                    c=re.sub('[^0-9a-zA-Z]', '_', c)
                sub=URIRef("http://w3id.org/kpionto/Member#"+c)
                if(c):
                    kg.add((sub,dl_prop.rollup,dl_member.Oceania))
    
def import_country_other(kg):
    kg.add((dl_member.Worldwide,dl_prop.rollup,dl_member.Other))



def import_region(kg):
    
    with open(DEFAULT_DIMENSIONS_DIR+"Geo.region","r",encoding="utf-8") as f: 
            values = f.read().split("\n")
            for c in values:
                if(' ' or ',' or '"') in c:
                    c=re.sub('[^0-9a-zA-Z]', '_', c)
                sub=URIRef("http://w3id.org/kpionto/Member#"+c)
                if(c):
                    kg.add((sub,RDF.type,dl_member.member))
                    kg.add((sub,dl_prop.inLevel,dl_lvl.region))
    


    


def import_all_country_region(kg):
    
    all_files = [f for f in os.listdir(DEFAULT_DIMENSIONS_DIRR+"Geo_country_region/") if os.path.isfile(os.path.join(DEFAULT_DIMENSIONS_DIRR+"Geo_country_region/", f))]

    for f in all_files:
        h=str(f)
        h=h.replace('Geo.region.',"")
        file= open(DEFAULT_DIMENSIONS_DIRR+"Geo_country_region/"+f,"r",encoding="utf-8")
        values = file.read().split("\n")
        ogg=URIRef("http://w3id.org/kpionto/Member#"+h)
        for c in values:
            if(' ' or ',' or '"') in c:
                c=re.sub('[^0-9a-zA-Z]', '_', c)
            sub=URIRef("http://w3id.org/kpionto/Member#"+c)
            
            if(c):
                kg.add((sub,dl_prop.rollup,ogg))
        h=""



def import_year(kg):
    with open("dimensions/Time.day","r",encoding="utf-8") as file:

        year=[]
        for x in file:
            valori = file.read().split("\n")
            for y in valori:
                year.append(y.split("-")[0])
        year=list(set(year))
    for c in year:
            if(' ' or ',' or '"') in c:
                c=re.sub('[^0-9a-zA-Z]', '_', c)
            sub=URIRef("http://w3id.org/kpionto/Member#"+c)
            if(c):
                kg.add((sub,RDF.type,dl_member.member))
                kg.add((sub,dl_prop.inLevel,dl_lvl.year))


def import_month(kg):
    with open("dimensions/Time.day","r",encoding="utf-8") as file:

        month=[]
        for x in file:
            valori = file.read().split("\n")
            for y in valori:
                month.append(y.split("-")[1])
        month=list(set(month))
    for c in month:
        if(' ' or ',' or '"') in c:
            c=re.sub('[^0-9a-zA-Z]', '_', c)
        sub=URIRef("http://w3id.org/kpionto/Member#"+c)
        if(c):
            kg.add((sub,RDF.type,dl_member.member))
            kg.add((sub,dl_prop.inLevel,dl_lvl.month))
        
def import_day(kg):

    with open("dimensions/Time.day","r",encoding="utf-8") as f:

        for x in f:
            values = f.read().split("\n")
            for c in values:
                if(' ' or ',' or '"') in c:
                    c=re.sub('[^0-9a-zA-Z]', '-', c)
                sub=URIRef("http://w3id.org/kpionto/Member#"+c)
                if(c):
                    kg.add((sub,RDF.type,dl_member.member))
                    kg.add((sub,dl_prop.inLevel,dl_lvl.day))
    

def import_iso2(graph):
    with open("dimensions/Geo.country_iso2","r",encoding="utf-8") as f:
            values = f.read().split("\n")
            for c in values:
                if(' ' or ',' or '"') in c:
                    c=re.sub('[^0-9a-zA-Z]', '_', c)
                sub=URIRef("http://w3id.org/kpionto/Member#"+c)
                if(c):
                    graph.add((sub,RDF.type,dl_member.member))
                    graph.add((sub,dl_prop.inLevel,dl_lvl.iso2))


def import_iso3(graph):
    with open("dimensions/Geo.country_iso3","r",encoding="utf-8") as f:
            values = f.read().split("\n")
            for c in values:
                if(' ' or ',' or '"') in c:
                    c=re.sub('[^0-9a-zA-Z]', '_', c)
                sub=URIRef("http://w3id.org/kpionto/Member#"+c)
                if(c):
                    graph.add((sub,RDF.type,dl_member.member))
                    graph.add((sub,dl_prop.inLevel,dl_lvl.iso3))

def import_country_iso2_country_iso3(graph):
    riga=[]
    iso2={}
    iso3={}
    with open("datasets/bing_covid-19_data.csv","r",encoding="utf-8") as f:

        for x in f:
            riga=x.split(",")
            key=riga[12]
            iso_2=riga[10]
            iso_3=riga[11]
            iso2[key]=iso_2
            iso3[key]=iso_3
        del iso2['country_region']
        del iso3['country_region']
        etl_iso2={}

        for y in iso2.keys():

            if iso2[y]!='':
                etl_iso2[y]=iso2[y]
        for x in etl_iso2.keys():
        
            c=etl_iso2[x]
            if(' ' or ',' or '"') in c:
                c=re.sub('[^0-9a-zA-Z]', '_', c)
            if(' ' or ',' or '"') in x:
                x=re.sub('[^0-9a-zA-Z]', '_', x)
            sub=URIRef("http://w3id.org/kpionto/Member#"+c)
            ogg=URIRef("http://w3id.org/kpionto/Member#"+x)
            graph.add((sub,OWL.sameAs,ogg))
            graph.add((ogg,OWL.sameAs,sub))

        etl_iso3={}
        for y in iso3.keys():

            if iso3[y]!='':
                etl_iso3[y]=iso3[y]
        for x in etl_iso3.keys():
            c=etl_iso3[x]
            if(' ' or ',' or '"') in c:
                c=re.sub('[^0-9a-zA-Z]', '_', c)
            if(' ' or ',' or '"') in x:
                x=re.sub('[^0-9a-zA-Z]', '_', x)
            sub=URIRef("http://w3id.org/kpionto/Member#"+c)
            ogg=URIRef("http://w3id.org/kpionto/Member#"+x)
            graph.add((sub,OWL.sameAs,ogg))
            graph.add((ogg,OWL.sameAs,sub))



def import_iso_region(graph):
    with open("dimensions/Geo.region_iso","r",encoding="utf-8") as f:
        values = f.read().split("\n")
        for c in values:
            if(' ' or ',' or '"') in c:
                c=re.sub('[^0-9a-zA-Z]', '_', c)
            sub=URIRef("http://w3id.org/kpionto/Member#"+c)
            if(c):
                graph.add((sub,RDF.type,dl_member.member))
                graph.add((sub,dl_prop.inLevel,dl_lvl.iso_region))

def import_region_iso(graph):
    with open("datasets/bing_covid-19_data.csv","r",encoding="utf-8") as f:
        riga=[]
        isoregion={}
        for x in f:
            riga=x.split(",")
            key=riga[13]
            iso_region=riga[14]
            isoregion[key]=iso_region
        del isoregion['admin_region_1']
        
        etl_isoregion={}
        for x in isoregion.keys():
            if isoregion[x]!='' or x=='':
                etl_isoregion[x]=isoregion[x]
        
        for x in etl_isoregion.keys():
            
            c=isoregion[x]
            if(' ' or ',' or '"') in c:
                c=re.sub('[^0-9a-zA-Z]', '_', c)
            if(' ' or ',' or '"') in x:
                x=re.sub('[^0-9a-zA-Z]', '_', x)

            sub=URIRef("http://w3id.org/kpionto/Member#"+c)
            ogg=URIRef("http://w3id.org/kpionto/Member#"+x)
            graph.add((sub,OWL.sameAs,ogg))
            
               
        

def main():

    
    graph= Graph()
    graph.bind('property',dl_prop)
    graph.bind('dimension',dl_dim)
    graph.bind('level',dl_lvl)
    graph.bind('member',dl_member)
    graph.add((dl_dim.geo,RDF.type,dl_dim.dimension))
    graph.add((dl_dim.Time,RDF.type,dl_dim.dimension))
    graph.add((dl_lvl.continent,RDF.type,dl_lvl.level))
    graph.add((dl_lvl.continent,dl_prop.inDimension,dl_dim.geo))
    graph.add((dl_lvl.country,RDF.type,dl_lvl.level))
    #consider if is better to define rollup for levels and anather rollup for members
    graph.add((dl_lvl.country,dl_prop.rollup,dl_lvl.continent))
    graph.add((dl_lvl.region,RDF.type,dl_lvl.level))
    graph.add((dl_lvl.region,dl_prop.rollup,dl_lvl.country))
    graph.add((dl_lvl.year,RDF.type,dl_lvl.level))
    graph.add((dl_lvl.year,dl_prop.inDimension,dl_dim.Time))
    graph.add((dl_lvl.mounth,RDF.type,dl_lvl.level))
    graph.add((dl_lvl.mounth,dl_prop.rollup,dl_lvl.year))
    graph.add((dl_lvl.day,RDF.type,dl_lvl.level))
    graph.add((dl_lvl.day,dl_prop.rollup,dl_lvl.month))
    graph.add((dl_dim.iso_code,RDF.type,dl_dim.dimension))
    graph.add((dl_lvl.iso2,RDF.type,dl_lvl.level))
    graph.add((dl_lvl.iso2,dl_prop.inDimension,dl_dim.iso_code))
    graph.add((dl_lvl.iso3,RDF.type,dl_lvl.level))
    graph.add((dl_lvl.iso3,dl_prop.inDimension,dl_dim.iso_code))
    graph.add((dl_lvl.iso_region,RDF.type,dl_lvl.level))
    #graph.add((OWL.sameAs,RDF.type,OWL.SymmetricProperty))


    import_continent(graph)
    import_country(graph)
    import_country_in_africa(graph)
    import_country_in_america(graph)
    import_country_in_europe(graph)
    import_country_in_asia(graph)
    import_country_in_oceania(graph)
    import_country_other(graph)
    import_region(graph)
    import_all_country_region(graph)
    import_year(graph)
    import_month(graph)
    import_day(graph)
    import_iso2(graph)
    import_iso3(graph)
    import_country_iso2_country_iso3(graph)
    import_iso_region(graph)
    import_region_iso(graph)
    #owlrl.DeductiveClosure(owlrl.CombinedClosure.RDFS_OWLRL_Semantics,datatype_axioms=True).expand(graph)


    graph.serialize(destination = "knowladge_graph.ttl", format = "turtle")



if __name__ == "__main__":
    main()