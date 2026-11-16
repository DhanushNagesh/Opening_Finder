{% test outcomes_sum_to_games(model, games='games', wins='wins', draws='draws', losses='losses') %}
select *
from {{ model }}
where {{ wins }} + {{ draws }} + {{ losses }} != {{ games }}
{% endtest %}
