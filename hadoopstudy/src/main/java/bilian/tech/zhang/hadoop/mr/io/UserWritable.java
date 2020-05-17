package bilian.tech.zhang.hadoop.mr.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 自定义数据类型
 * 实现 writable 接口
 * value
 *
 * equals and hashcode 重写，方便数据的比较和reduce时的划分
 */
public class UserWritable implements Writable {

    private String userID;
    private String userName;

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public UserWritable(String userID, String userName) {
       setData(userID, userName);
    }

    public UserWritable(){}

    public void setData(String userID, String userName) {
        this.userID = userID;
        this.userName = userName;
    }



    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userID);
        out.writeUTF(userName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.userID = in.readUTF();
        this.userName = in.readUTF();
    }


    @Override
    public String toString() {
        return "UserWritable{" +
                "userID='" + userID + '\'' +
                ", userName='" + userName + '\'' +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserWritable that = (UserWritable) o;
        return Objects.equals(userID, that.userID) &&
                Objects.equals(userName, that.userName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userID, userName);
    }
}
