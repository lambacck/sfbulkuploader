(function($) {
    var url = null;
    var log_containter = null;
    var get_status = function() {
        $.ajax(url, {
            dataType: 'json',
            cache: false
        }).done(function(data) {
            log_container.html(data.log_html);
            if (data.completed) {
                $('#throbber').remove();
                $('#in-progress').hide();
            } else {
                setTimeout(get_status, 5000);
            }
        }).fail(function() {
            setTimeout(get_status, 5000);
        });
    };
    $(function() {
        log_container = $('#log-container');
        url = window.location.pathname.replace('progress', 'status');

        get_status();

    });
})(jQuery);
