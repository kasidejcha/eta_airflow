select *
from link_data 
where link_id IN (
	select CAST(unnest(string_to_array(replace(replace(link_ids, '{', ''), '}', ''), ',')) as bigint)
	from master_link
	where master_link = 'z0494z0495'
)

