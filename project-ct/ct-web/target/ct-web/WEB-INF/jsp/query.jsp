<%--
  Created by IntelliJ IDEA.
  User: 诚实的狮子
  Date: 2020/11/7
  Time: 13:43
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <!-- 上述3个meta标签*必须*放在最前面，任何其他内容都*必须*跟随其后！ -->
    <title>Bootstrap 101 Template</title>

    <!-- Bootstrap -->
    <link href="bootstrap/css/bootstrap.css" rel="stylesheet">

    <!-- HTML5 shim and Respond.js for IE8 support of HTML5 elements and media queries -->
    <!-- WARNING: Respond.js doesn't work if you view the page via file:// -->

</head>
<body>
<h1>你好，世界！</h1>

<form>
    <div class="form-group">
        <label for="tel">电话号码</label>
        <input type="text" class="form-control" id="tel" placeholder="请输入电话号码">
    </div>
    <div class="form-group">
        <label for="calltime">日期</label>
        <input type="text" class="form-control" id="calltime" placeholder="请输入查询时间">
    </div>

    <button type="button" class="btn btn-default" onclick="queryData()" >Submit</button>
</form>


<!-- jQuery (necessary for Bootstrap's JavaScript plugins) -->
<script src="jquery/jquery-2.1.1.min.js"></script>
<!-- Include all compiled plugins (below), or include individual files as needed -->


<script src="bootstrap/js/bootstrap.js"></script>

<script>

    function queryData() {

        window.location.href = "/view?tel=" + $("#tel").val() + "&calltime=" + $("#calltime").val();

    }

</script>
</body>
</html>