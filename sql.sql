 WITH RECURSIVE
 sources(source, count) as (
    select source,count(*) as count
    from glendale_small_ways_vertices group by source
    ),
 targets(target,count) as (
    select target,count(*) as count
    from glendale_small_ways_vertices group by target
    ),
 possible_interiors as (
    select w.*, s.count as scount, t.count as tcount
    from glendale_small_ways_vertices w
    join targets t on (w.target=t.target)
    join sources s on (w.source = s.source)
    where t.count=1 and s.count=1
    ),
 interiors as (
     select pi.*
     from possible_interiors pi
     join sources s on (pi.target=s.source)
     join targets t on (pi.source=t.target)
     where s.count=1 and t.count=1
     ),
 possible_starts as (
    select w.*, s.count as scount, t.count as tcount
    from glendale_ways_vertices w
    join targets t on (w.target=t.target)
    join sources s on (w.source = s.source)
    where t.count=1
    ),
 starts as (
    select ps.*
    from possible_starts ps
    where ps.scount>1
   union
    select ps.*
    from possible_starts ps
    join targets t on (ps.source=t.target)
    where ps.scount=1 and t.count>1
   union
    select ps.*
    from possible_starts ps
    join possible_interiors pi on (ps.source=pi.target)
    where ps.name != pi.name and ps.scount=1 and pi.scount=1
   union
    select ps.*
    from possible_starts as ps
    join possible_starts  psi  on (ps.source=psi.target and ps.name != psi.name)
    ),
possible_ends as (
    select w.*, s.count as scount, t.count as tcount
    from glendale_ways_vertices w
    join targets t on (w.target=t.target)
    join sources s on (w.source = s.source)
    where s.count=1
    ),
ends as (
    select pe.*
    from possible_ends pe
    where pe.scount>1
   union
    select pe.*
    from possible_ends pe
    join targets t on (pe.source=t.target)
    where pe.scount=1 and t.count>1
    union
    select pe.*
    from possible_ends pe
    join possible_interiors pi on (pe.target=pi.source)
    where pe.name != pi.name and pe.scount=1 and pi.scount=1
    union
    select pe.*
    from possible_ends as pe
    join possible_starts  psi  on (pe.target=psi.source and pe.name != psi.name)
 ),
 search_graph as (
    select g.gid, g.length_m, g.name, g.source, g.target,
    1 as depth,
    array[g.gid] as path,
    st_asewkt(g.the_geom) as segments,
    false as cycle
    from ends g
   union all
    select g.gid, g.length_m + sg.length_m,
    sg.name, g.source, sg.target,
    sg.depth+1 as depth,
    g.gid || sg.path as path,
    st_asewkt( st_makeline( g.the_geom, sg.segments )),
    g.gid = ANY(sg.path) as cycle
    from interiors g -- recurse on interiors
    join search_graph sg on
      (g.target=sg.source -- interior target -> chain source
       and g.name=sg.name)-- but same street name too please
    where sg.depth < 100 and not sg.cycle -- stop guards
  ),
 gid_paths as (select unnest(sg.path) as node,depth
    from search_graph sg ),
 gid_max_depth as (
    select node,max(depth) as depth
    from gid_paths group by node ),

 distinct_paths as (
   select distinct path
   from search_graph sg
   join gid_max_depth gm
        on (gm.depth=sg.depth and
            gm.node in (select unnest(sg.path)))
   ),
 segments as (
    select c.gid, g.*, c.length_m, c.name, g.source, c.target, c.depth+1 as depth,
    g.gid || c.path as path,
    ST_SimplifyPreserveTopology(
     ST_GeomFromEWKT(st_asewkt(st_makeline(g.the_geom,
                                           c.segments))),
     0.0000001) as the_geom
    from search_graph c
    join distinct_paths dp on (c.path=dp.path)
    join starts g -- add start nodes to chain
         on (g.target=c.source  --start.target == source
             and g.name=c.name) -- same name please
    )
select * from segments;

#######################################################
create table small_vertices as
with recursive
ways_vertices as (
  select
  ST_X(ST_GeomFromEWKT(ST_AsEWKT(ST_LineInterpolatePoint(
    w.the_geom, 0.5)))) as lon,
  ST_Y(ST_GeomFromEWKT( ST_AsEWKT(ST_LineInterpolatePoint(
    w.the_geom,
    0.5)))) as lat  ,
  ST_GeomFromEWKT(  ST_AsEWKT(ST_LineInterpolatePoint(
    w.the_geom,
    0.5))) as the_geom
  from small_segments w
  )
select * from ways_vertices
;

##########################################################
create table glendale_small_ways_vertices as
with recursive
ways_vertices as (
  select w.gid, w.osm_id, w.length_m, w.name ,w.source, w.target,
  ST_X(ST_GeomFromEWKT( ST_AsEWKT(ST_LineInterpolatePoint(
    ST_GeomFromEWKT(st_asewkt(st_makeline(ST_MakePoint(w.x1, w.y1), ST_MakePoint(w.x2, w.y2)))),
    0.5)))) as lon,
  ST_Y(ST_GeomFromEWKT( ST_AsEWKT(ST_LineInterpolatePoint(
    ST_GeomFromEWKT(st_asewkt(st_makeline(ST_MakePoint(w.x1, w.y1), ST_MakePoint(w.x2, w.y2)))),
    0.5)))) as lat  ,
  ST_GeomFromEWKT(  ST_AsEWKT(ST_LineInterpolatePoint(
    ST_GeomFromEWKT(st_asewkt(st_makeline(ST_MakePoint(w.x1, w.y1), ST_MakePoint(w.x2, w.y2)))),
    0.5))) as the_geom
  from glendale_small_ways w
  )
select * from ways_vertices
;

#################################################################
-- now with only one-way links for each curbside, make linegraph

drop table if exists curbs_v2_linegraph;
SELECT * into curbs_v2_linegraph FROM pgr_lineGraph(
    'SELECT curbid as id, source, target, cost_s as cost, reverse_cost_s as reverse_cost FROM curbs_v2_graph'
);

-- now fix "two-way" links in the line graph to be two, one-way links
-- all two way links are now positive to positive...no more negative source or sink
drop sequence if exists curbs_v2_linegraph_serial;
create sequence curbs_v2_linegraph_serial;

drop table if exists new_curbs_v2_linegraph;

create table new_curbs_v2_linegraph as
with
el_st as (
    SELECT nextval('curbs_v2_linegraph_serial') as id,
       source, target, cost, -1 as reverse_cost
    FROM curbs_v2_linegraph
    where cost >0 and source != -target),
el_ts as (
    SELECT nextval('curbs_v2_linegraph_serial') as id,
       target as source, source as target, reverse_cost as cost, -1 as reverse_cost
    FROM curbs_v2_linegraph
    where reverse_cost >0 and source != -target),
big_union as (
    select * from el_st
    union
    select * from el_ts
    order by id)
select * from big_union;


-- now join to draw lines between linegraph links

alter table new_curbs_v2_linegraph add column geom geometry(LINESTRING,4326);

with
curb_midpoint as (
   select ST_LineInterpolatePoint(cg.curb_geom,0.5) as midpoint, curbid as id
   from curbs_v2_graph cg
   ),
endpoints_line as (
   select ST_MakeLine(c_s.midpoint,c_t.midpoint) as geom, nel.id
   from new_curbs_v2_linegraph nel
   join curb_midpoint c_s on (nel.source=c_s.id)
   join curb_midpoint c_t on (nel.target=c_t.id)
   )
update new_curbs_v2_linegraph nel
set (geom) =
   (select geom
    from endpoints_line el
    where el.id = nel.id);

-- add length of source link to curbs_v2_linegraph.  Cost of getting
-- from source to destination is cost of traversing source.  By
-- definition

ALTER TABLE new_curbs_v2_linegraph ADD PRIMARY KEY (id);

create index new_curbs_v2_linegraph_source_idx on new_curbs_v2_linegraph (source);
create index new_curbs_v2_linegraph_target_idx on new_curbs_v2_linegraph (target);
create index new_curbs_v2_linegraph_geom_idx on new_curbs_v2_linegraph using gist(geom);

-- alter table new_curbs_v2_linegraph add foreign key (source) references curbs_v2_graph (curbid);
-- alter table new_curbs_v2_linegraph add foreign key (target) references curbs_v2_graph (curbid);


alter table new_curbs_v2_linegraph add column source_length_m double precision;

with sourcelen (id,source,len) as (
   select lc.id,lc.source,c.length_m
   from new_curbs_v2_linegraph lc
   join curbs_v2_graph c on (c.curbid=lc.source)
   )
update new_curbs_v2_linegraph nel
set (source_length_m) =
   (select len
    from sourcelen sl
    where sl.id=nel.id);

alter table new_curbs_v2_linegraph add column target_length_m double precision;

with targetlen (id,target,len) as (
   select lc.id,lc.target,c.length_m
   from new_curbs_v2_linegraph lc
   join curbs_v2_graph c on (c.curbid=lc.target)
   )
update new_curbs_v2_linegraph nel
set (target_length_m) =
   (select len
    from targetlen sl
    where sl.id=nel.id);


#####################################


  drop sequence if exists curbgraph_v2_serial;
  create sequence curbgraph_v2_serial;

  drop table if exists curbs_v2_graph cascade;

  with
  tform as (
      select id, st_transform(the_geom,32611) as geom,reverse_cost
      from new_glendale_ways),
  rhs as (
      select ST_Reverse(ST_Transform (
               ST_OffsetCurve(
                  geom,
                  -2)  -- 2 meters offset, 6 feet-ish, reverse of orig direction
                  ,4326)) as geom, --  back to source geometry, (need reverse)
             id
      from tform),
  lhs_twoway as (
      select ST_Reverse(ST_Transform (
               ST_OffsetCurve(
                  geom,
                  2)  -- 2 meters offset, 6 feet-ish, same sense
                  ,4326)) as geom, --  back to source geometry, (need reverse because lhs)
             id
      from tform
      where reverse_cost > 0),
  lhs_oneway as (
      select ST_Transform (
               ST_OffsetCurve(
                  geom,
                  2)  -- 2 meters offset, 6 feet-ish, same sense
                  ,4326) as geom, --  back to source geometry, no rev. for 1-way str
             id
      from tform
      where reverse_cost < 0),
  lhs as (
      select * from lhs_twoway
      union
      select * from lhs_oneway),
  lhscurbs as (
    select
      nextval('curbgraph_v2_serial') as curbid,
      'lhs' as curbside,
      a.id,
      a.osm_ids,
      a.tag_ids,
      a.length,
      a.length_m,
      a.name,
      case when a.one_way=1 then  a.source else a.target end  as source,
      case when a.one_way=1 then  a.target else a.source end  as target,
      a.source_osm,
      a.target_osm,
      case when a.one_way=1 then  a.cost else a.reverse_cost end  as cost,
      -1 as reverse_cost,
      case when a.one_way=1 then  a.cost_s else a.reverse_cost_s end  as cost_s,
      -1 as reverse_cost_s,
      a.rule,
      a.one_way,
      a.oneway,
      a.maxspeed_forward,
      a.maxspeed_backward,
      a.priority,
      a.the_geom,
      cg.geom as curb_geom
    from new_glendale_ways a
    left outer join lhs cg on (a.id=cg.id)
    where one_way=1 or reverse_cost>0),
  rhscurbs as (
    select
      nextval('curbgraph_v2_serial') as curbid,
      'rhs' as curbside,
      a.id,8
      a.osm_ids,
      a.tag_ids,
      a.length,
      a.length_m,
      a.name,
      a.source,
      a.target,
      a.source_osm,
      a.target_osm,
      a.cost,
      -1 as reverse_cost,
      a.cost_s,
      -1 as reverse_cost_s,
      a.rule,
      a.one_way,
      a.oneway,
      a.maxspeed_forward,
      a.maxspeed_backward,
      a.priority,
      a.the_geom,
      cg.geom as curb_geom
    from new_glendale_ways a
    left outer join rhs cg on (a.id=cg.id)),
  curbs as (
    select * from rhscurbs
    union
    select * from lhscurbs)
  select * into curbs_v2_graph from curbs;
