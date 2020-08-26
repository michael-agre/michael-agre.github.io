/*--------------------------------------
Author: Michael Agre
Date: 06/28/2019
Version: 3.3
Description: This SQL takes the most recent 10000 US-POS-Post
vectors seen in production and uploads them to an AP_RISK
table in a format that can be used by HCI web applications
to run batch vector tests. Will be used to ensure accuracy
of new blaze deployments for US-POS-Post Strategy.
--------------------------------------*/

DELETE FROM AP_RISK.SIM_POS_INPUT_POST; -- Empty table first, want fresh vectors each day.
INSERT INTO AP_RISK.SIM_POS_INPUT_POST

WITH t AS (
  SELECT
  H.ID_VECTOR,
  case 
    when SVA.TEXT_VECTOR_ATTR_FULL_PATH like 'GENRESULT%' then
      substr(sva.STRATEGY_CODE, instr(sva.STRATEGY_CODE, '(', 1)+1,instr(sva.STRATEGY_CODE, ')', 1)-instr(sva.STRATEGY_CODE, '(', 1)-1)||substr(SVA.TEXT_VECTOR_ATTR_FULL_PATH, 10)
    else sva.TEXT_VECTOR_ATTR_FULL_PATH 
  end AS attribute_id, 
  SVA.CODE_VECTOR_ATTR_DATATYPE AS datatype, 
  D.TEXT_VALUE, 
  D.DTIME_VALUE, 
  D.NUM_VALUE, 
  SVA.FLAG_ARRAY_DATATYPE AS array_flag,
  H.NUM_GROUP_POSITION_1 AS pos1,
  H.NUM_GROUP_POSITION_2 AS pos2, 
  H.NUM_GROUP_POSITION_3 AS pos3
  from OWNER_DWH.F_SCORING_VECTOR_H_TT H
  join OWNER_DWH.F_SCORING_VECTOR_D_TT D on D.SKF_SCORING_VECTOR_H_TT = H.SKF_SCORING_VECTOR_H_TT
  join OWNER_DWH.DC_SCORING_VECTOR_ATTRIBUTE SVA on D.SKP_SCORING_VECTOR_ATTRIBUTE = SVA.SKP_SCORING_VECTOR_ATTRIBUTE
  where H.id_vector in 
    (
    SELECT ID_VECTOR FROM (
      SELECT ROWNUM AS RN, id_vector FROM
        (
          --Only select Post input vectors
          SELECT h.ID_VECTOR 
          FROM OWNER_DWH.F_SCORING_VECTOR_H_TT H
          join OWNER_DWH.F_SCORING_VECTOR_D_TT D on D.SKF_SCORING_VECTOR_H_TT = H.SKF_SCORING_VECTOR_H_TT
          WHERE d.DATE_EFFECTIVE > sysdate - 4
          and D.SKP_SCORING_VECTOR_ATTRIBUTE = 10301 AND D.TEXT_VALUE = 'OFFER'
          and h.ID_VECTOR in (
            --Only select vectors for Phase 1 applications
            SELECT h.ID_VECTOR
            FROM OWNER_DWH.F_SCORING_VECTOR_H_TT H
            join OWNER_DWH.F_SCORING_VECTOR_D_TT D on D.SKF_SCORING_VECTOR_H_TT = H.SKF_SCORING_VECTOR_H_TT
            WHERE d.DATE_EFFECTIVE > sysdate - 4
            and D.SKP_SCORING_VECTOR_ATTRIBUTE = 9977 AND D.TEXT_VALUE = 'SPRINT'         
          )
          --ORDER BY H.ID_VECTOR DESC
        )
      WHERE ROWNUM <= 10000
    ) -- Selects 10000 applications in the last 14 days which we will use to find vectors.
  )
)-- Select all attributes that will be used in final product. This has all information we need, 
--  now we just format it into a readable format for web application.

 
select row_number() over (partition by id_credit order by as_full_path) as id, a.* from --Necessary for format correctness 
(
select distinct
t.ID_VECTOR as id_credit, 
case
  when (pos3 is not null and array_flag = 'Y') then SUBSTR(t.attribute_id, 1, INSTR(t.attribute_id, '[', 1, 1)) || (pos1 - 1)
    || SUBSTR(t.attribute_id, INSTR(t.attribute_id, ']', 1, 1), INSTR(t.attribute_id, '[', 1, 2) - INSTR(t.attribute_id, ']', 1, 1) + 1) || (pos2 - 1)
    || SUBSTR(t.attribute_id, INSTR(t.attribute_id, ']', 1, 2), INSTR(t.attribute_id, '[', 1, 3) - INSTR(t.attribute_id, ']', 1, 2) + 1) || (pos3 - 1)
    || SUBSTR(t.attribute_id, INSTR(t.attribute_id, ']', 1, 3), length(t.attribute_id))
  when (pos2 is not null and array_flag = 'Y') then SUBSTR(t.attribute_id, 1, INSTR(t.attribute_id, '[', 1, 1)) || (pos1 - 1)
    || SUBSTR(t.attribute_id, INSTR(t.attribute_id, ']', 1, 1), INSTR(t.attribute_id, '[', 1, 2) - INSTR(t.attribute_id, ']', 1, 1) + 1) || (pos2 - 1)
    || SUBSTR(t.attribute_id, INSTR(t.attribute_id, ']', 1, 2), length(t.attribute_id))
  when (pos1 is not null and array_flag = 'Y') then SUBSTR(t.attribute_id, 1, INSTR(t.attribute_id, '[', 1, 1)) || (pos1 - 1)
    || SUBSTR(t.attribute_id, INSTR(t.attribute_id, ']', 1, 1), length(t.attribute_id))
  else t.attribute_id
end
as as_full_path, -- format array full paths. Insert array numbers into given fullpath 
case
  when t.datatype = 'c' then 'c'
  when t.datatype = 'n' or t.datatype = 'b' then 'n'
  when t.datatype = 'dt' or t.datatype = 'd' then 'd'
end as data_type, -- Tells web app data type of attribute
case
  when t.datatype = 'n' then (
    case 
      when t.attribute_id = 'idRequest' then t.id_vector -- without this, id_credit in output is not unique
      else t.num_value
    end
  )
  when t.datatype = 'b' then (
    case
      when t.text_value = 'true' then 1
      else 0
    end
  ) -- boolean values need to be numbers 0 or 1 to be used by web app
  else null
end as nvalue,
case
  when t.datatype = 'c' then t.text_value
  else null
end as cvalue,
case
  when t.datatype = 'dt' or t.datatype = 'd' then dtime_value
  else null 
end as dvalue -- insert actual values in the correct format.
from t
order by ID_VECTOR
) a
where a.as_full_path not like ('outputData%')
and a.as_full_path not like ('postResult%')
and not (a.as_full_path like ('%termConstraints[].term') and a.as_full_path like ('credit.history%')); -- exclude attributes that should not be present in Post input vectors.

COMMIT;
