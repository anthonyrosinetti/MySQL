function get_dashed_date(date) {
    if (date instanceof Date) {
        var year = date.getFullYear();
        var month = date.getMonth() + 1;
        if (month < 10) {
            month = '0' + month.toString();
        }
        var day = date.getDate();
        if (day < 10) {
            day = '0' + day.toString();
        }
        return year + '-' + month + '-' + day;
    }
}

function get_start_date() {
    if (dataform.projectConfig.vars.start_date) {
        return "'" + dataform.projectConfig.vars.start_date + "'";
    }
    const today = new Date(Date.now());
    var relative_start_date = 1;
    if (dataform.projectConfig.vars.relative_start_date) {
        relative_start_date = parseInt(dataform.projectConfig.vars.relative_start_date)
    }
    const start_date = new Date(today.getFullYear(), today.getMonth(), today.getDate() - relative_start_date);
    return "'" + get_dashed_date(start_date) + "'";
}

function get_end_date() {
    if (dataform.projectConfig.vars.end_date) {
        return "'" + dataform.projectConfig.vars.end_date + "'";
    }
    const today = new Date(Date.now());
    var relative_end_date = 0;
    if (dataform.projectConfig.vars.relative_end_date) {
        relative_end_date = parseInt(dataform.projectConfig.vars.relative_end_date)
    }
    const end_date = new Date(today.getFullYear(), today.getMonth(), today.getDate() - relative_end_date);
    return "'" + get_dashed_date(end_date) + "'";
}

module.exports = {
    get_start_date,
    get_end_date
};
