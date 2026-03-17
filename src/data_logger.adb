--  This package provides data logging Carlo Gavazzi Energy meter via
--  WaveShare RS455 to POE Ethernet converter. It assumes that energy meter
--  data is provided by an MQTT broker.

--  Author    : David Haley
--  Created   : 04/03/2026
--  Last_Edit : 16/03/2026

--  20260315: Ported to MQTT_Client;

with Ada.Text_IO; use Ada.Text_IO;
with Ada.Text_IO.Unbounded_IO; use Ada.Text_IO.Unbounded_IO;
with Ada.Directories; use Ada.Directories;
with Ada.Calendar; use Ada.Calendar;
with Ada.Calendar.Time_Zones; use Ada.Calendar.Time_Zones;
with Ada.Calendar.Arithmetic; use Ada.Calendar.Arithmetic;
with Ada.Calendar.Formatting; use Ada.Calendar.Formatting;
with Ada.Strings; use Ada.Strings;
with Ada.Strings.Maps; use Ada.Strings.Maps;
with Ada.Strings.Maps.Constants; use Ada.Strings.Maps.Constants;
with Ada.Strings.Fixed; use Ada.Strings.Fixed;
with Ada.Strings.Unbounded; use Ada.Strings.Unbounded;
with Interfaces; use Interfaces;
with GNATCOLL.Os; use GNATCOLL.Os;
with GNATCOLL.Buffer; use GNATCOLL.Buffer;
with GNATCOLL.JSON; use GNATCOLL.JSON;
with DJH.Date_and_Time_Strings; use DJH.Date_and_Time_Strings;
with DJH.Events_and_Errors; use DJH.Events_and_Errors;
with MQTT_Client; use MQTT_Client;

package body Data_Logger is
   Logging_File : File_Type;
   Log_Interval : constant Day_Duration := 60.0;
   --  Create a log entry at 1 minute inerevals
   File_Commit_Interval : constant Duration := 3600.0;
   --  Commit logging files once per hour

   type Fixed_1 is delta 0.1 digits 14;
   type Fixed_2 is delta 0.01 digits 14;
   type Fixed_3 is delta 0.001 digits 14;

   type Log_Data is record
      Message_Time : Time;
      Voltage, Power, VA, VAR, Frequency, Import_kWh, Export_kWh : Fixed_1;
      Current, Power_Factor : Fixed_3;
      Hours_Counter : Fixed_2;
   end record; -- Log_Data;

   package Duration_IO is new Ada.Text_IO.Fixed_IO (Duration);
   package Fixed_1_IO is new Ada.Text_IO.Decimal_IO (Fixed_1);
   package Fixed_2_IO is new Ada.Text_IO.Decimal_IO (Fixed_2);
   package Fixed_3_IO is new Ada.Text_IO.Decimal_IO (Fixed_3);

   task Receiver is
      --  Declaration of Receiver task
      entry Start (Broker_Host, User_Name, Password, Topic : String);
      entry Get (Data_Copy : out Log_Data);
      entry Stop;
   end Receiver;

   function Convert (N : Unsigned_16) return Fixed_1 is

      --  Converts to Fixed_1
      Result   : Fixed_1;
      Sign_Bit : constant Unsigned_16 := 16#8000#;

   begin -- Convert
      if (N and Sign_Bit) = 0 then
         Result := Fixed_1 (N);
      else
         Result := 0.0 - Fixed_1 (not N + 1);
      end if; -- (N and Sign_Bit)
      return Result / 10.0;
   end Convert;

   function Convert (N : Unsigned_16) return Fixed_3 is

      --  Converts to Fixed_3.

      Result   : Fixed_3;
      Sign_Bit : constant Unsigned_16 := 16#8000#;

   begin -- Convert
      if (N and Sign_Bit) = 0 then
         Result := Fixed_3 (N);
      else
         Result := 0.0 - Fixed_3 (not N + 1);
      end if; -- (N and Sign_Bit)
      return Result / 1000.0;
   end Convert;

   function Convert (N : Unsigned_32) return Fixed_1 is

      --  Converts to Fixed_1.

      Sign_Bit : constant Unsigned_32 := 16#8000_0000#;
      Result   : Fixed_1;

   begin -- Convert
      if (N and Sign_Bit) = 0 then
         Result := Fixed_1 (N);
      else
         Result := 0.0 - Fixed_1 (not N + 1);
      end if; -- (N and Sign_Bit)
      return Result / 10.0;
   end Convert;

   function Convert (N : Unsigned_32) return Fixed_2 is

      --  Converts to Fixed_2.

      Sign_Bit : constant Unsigned_32 := 16#8000_0000#;
      Result   : Fixed_2;

   begin -- Convert
      if (N and Sign_Bit) = 0 then
         Result := Fixed_2 (N);
      else
         Result := 0.0 - Fixed_2 (not N + 1);
      end if; -- (N and Sign_Bit)
      return Result / 100.0;
   end Convert;

   function Convert (N : Unsigned_32) return Fixed_3 is

      --  Converts to Fixed_3.

      Sign_Bit : constant Unsigned_32 := 16#8000_0000#;
      Result   : Fixed_3;

   begin -- Convert
      if (N and Sign_Bit) = 0 then
         Result := Fixed_3 (N);
      else
         Result := 0.0 - Fixed_3 (not N + 1);
      end if; -- (N and Sign_Bit)
      return Result / 1000.0;
   end Convert;

   task body Receiver is

      --  Receiver assumes that meter data will be received at approximately
      --  10 s intervals from the MQTT broker. Some measurements are averaged
      --  over the last six samples approximately one minute. Other
      --  measurements represent the last received.

      type Buffer_Elements is record
         Voltage, Power, VA, VAR, Frequency : Fixed_1;
         Current, Power_Factor : Fixed_3;
      end record; -- Buffer_Elements
      type Buffer_Indices is mod 6;
      type Buffers is array (Buffer_Indices) of Buffer_Elements;

      procedure Writer (Handle : MQTT_Handle;
                        Buffer : in out Buffers;
                        Log_Datum : in out Log_Data;
                        Write_At : in out Buffer_Indices) is

         Data_J : String := Receive (Handle, 30.0); -- wait 30 s to receive
         Prefix : constant String := "Writer - ";

      begin -- Writer
         if Data_J'Length > 0 then
            declare -- Parse JSON
               Data : constant Read_Result := Read (Data_J);
               Number : Long_Integer;
            begin -- Parse JSON
               if Data.Success then
                  begin -- Message_Time exception block
                        Log_Datum.Message_Time :=
                        Value (Get (Data.Value, "CN_Time"), 8 * 60);
                        --  8 hours is the offset to Chinese time, Message_Time
                        --  is UTC.
                  exception
                     when E : others =>
                        raise Data_Error with Prefix & "Message_Time error";
                  end; -- Message_Time exception block
                  begin -- Voltage exception block
                     Number := Get (Data.Value, "Voltage");
                     Buffer (Write_At).Voltage :=
                       Convert (Unsigned_32 (Number));
                  exception
                     when E : others =>
                        raise Data_Error with Prefix & "Voltage error";
                  end; -- Voltage exception block
                  begin -- Current exception block
                     Number := Get (Data.Value, "Current");
                     Buffer (Write_At).Current :=
                       Convert (Unsigned_32 (Number));
                  exception
                     when E : others =>
                        raise Data_Error with Prefix & "Current error";
                  end; -- Current exception block
                  begin -- Power exception block
                     Number := Get (Data.Value, "Power");
                     Buffer (Write_At).Power :=
                       Convert (Unsigned_32 (Number));
                  exception
                     when E : others =>
                        raise Data_Error with Prefix & "Power error";
                  end; -- Power exception block
                  begin -- VA exception block
                     Number := Get (Data.Value, "VA");
                     Buffer (Write_At).VA := Convert (Unsigned_32 (Number));
                  exception
                     when E : others =>
                        raise Data_Error with Prefix & "VA error";
                  end; -- VA exception block
                  begin -- VAR exception block
                     Number := Get (Data.Value, "VAR");
                     Buffer (Write_At).VAR := Convert (Unsigned_32 (Number));
                  exception
                     when E : others =>
                        raise Data_Error with Prefix & "VAR error";
                  end; -- VAR exception block
                  begin -- Power_Factor exception block
                     Number := Get (Data.Value, "Power_Factor");
                     Buffer (Write_At).Power_Factor :=
                       Convert (Unsigned_16 (Number));
                  exception
                     when E : others =>
                        raise Data_Error with Prefix & "Power_Factor error";
                  end; -- Power_Factor exception block
                  begin -- Frequency exception block
                     Number := Get (Data.Value, "Frequency");
                     Buffer (Write_At).Frequency :=
                       Convert (Unsigned_16 (Number));
                  exception
                     when E : others =>
                        raise Data_Error with Prefix & "Frequency error";
                  end; -- Frequency exception block
                  begin -- Import_kWh exception block
                     Number := Get (Data.Value, "Import_kWh");
                     Log_Datum.Import_kWh := Convert (Unsigned_32 (Number));
                  exception
                     when E : others =>
                        raise Data_Error with Prefix & "Import_kWh error";
                  end; -- Import_kWh exception block
                  begin -- Export_kWh exception block
                     Number := Get (Data.Value, "Export_kWh");
                     Log_Datum.Export_kWh := Convert (Unsigned_32 (Number));
                  exception
                     when E : others =>
                        raise Data_Error with Prefix & "Export_kWh error";
                  end; -- Export_kWh exception block
                  begin -- Hours_Counter exception block
                     Number := Get (Data.Value, "Hours_Counter");
                     Log_Datum.Hours_Counter := Convert (Unsigned_32 (Number));
                  exception
                     when E : others =>
                        raise Data_Error with Prefix & "Hours_Counter error";
                  end; -- Hours_Counter exception block
                  Write_At := @ + 1;
               else
                  raise Data_Error with Prefix & " JSON error at" &
                    Data.Error.Column'Img & " " &
                    To_String (Data.Error.Message);
               end if; -- Data.Success
            end; -- Parse JSON
         end if; -- Data_J'Lenght > 0
      end Writer;

      Prefix : constant String := "Receiver - ";
      Run_Receiver : Boolean := True;
      Handle : MQTT_Handle;
      Buffer : Buffers;
      Write_At, Previous : Buffer_Indices := Buffer_Indices'First;
      Log_Datum : Log_Data;

   begin -- Receiver
      accept Start (Broker_Host, User_Name, Password, Topic : String) do
         Connect_Rx (Broker_Host, User_Name, Password, Topic, Handle);
         loop -- Until Buffer has been filled
            Writer (Handle, Buffer, Log_Datum, Write_At);
            exit when Write_At = Buffer_Indices'First and
              Previous = Buffer_Indices'Last;
            --  Allows for receive timing out
            Previous := Write_At;
         end loop; -- Until Buffer has been filled
      end Start;
      while Run_Receiver loop
         select
            accept Stop do
               Disconnect (Handle);
               Run_Receiver := False;
            end Stop;
         or
            accept Get (Data_Copy : out Log_Data) do
               --  Calculate averages
               Log_Datum.Voltage := 0.0;
               Log_Datum.Current := 0.0;
               Log_Datum.Power := 0.0;
               Log_Datum.VA := 0.0;
               Log_Datum.VAR := 0.0;
               Log_Datum.Power_Factor := 0.0;
               Log_Datum.Frequency := 0.0;
               for I in Buffer_Indices loop
                  Log_Datum.Voltage := @ + Buffer (I).Voltage;
                  Log_Datum.Current := @ + Buffer (I).Current;
                  Log_Datum.Power := @ + Buffer (I).Power;
                  Log_Datum.VA :=  @ + Buffer (I).VA;
                  Log_Datum.VAR :=  @ + Buffer (I).VAR;
                  Log_Datum.Power_Factor :=  @ + Buffer (I).Power_Factor;
                  Log_Datum.Frequency :=  @ + Buffer (I).Frequency;
               end loop; -- I in Buffer_Indices
               Log_Datum.Voltage := @ / Fixed_1 (Buffer_Indices'Modulus);
               Log_Datum.Current := @ / Fixed_3 (Buffer_Indices'Modulus);
               Log_Datum.Power :=  @ / Fixed_1 (Buffer_Indices'Modulus);
               Log_Datum.VA := @ / Fixed_1 (Buffer_Indices'Modulus);
               Log_Datum.VAR := @ / Fixed_1 (Buffer_Indices'Modulus);
               Log_Datum.Power_Factor := @ / Fixed_3 (Buffer_Indices'Modulus);
               Log_Datum.Frequency := @ / Fixed_1 (Buffer_Indices'Modulus);
               Data_Copy := Log_Datum;
            end Get;
         else
            Writer (Handle, Buffer, Log_Datum, Write_At);
         end select;
      end loop; -- Run_receiver
   exception
      when E : others =>
         Put_Error (Prefix & "unhandled exception", E);
         raise;
   end Receiver;

   function On_The_Hour (T : in Time) return Time is
      --  Effectively rounds T down such that minutes and seconds are zero

      Year : Year_Number;
      Month : Month_Number;
      Day : Day_Number;
      This_Hour : Hour_Number;
      Minute : Minute_Number;
      Second : Second_Number;
      Sub_Second : Second_Duration;
      Leap_Second : Boolean;

   begin -- On_The_Hour
      Split (T, Year, Month, Day, This_Hour, Minute, Second, Sub_Second,
         Leap_Second, UTC_Time_Offset (T));
      return Time_Of (Year, Month, Day, This_Hour, 0, 0, 0.0, Leap_Second,
                      UTC_Time_Offset (T));
   end On_The_Hour;

   protected type File_Commit_Times is

      procedure Set_Next_File_Commit;
      --  Sets time for next file commit

      function Get_Next_File_Commit return Time;
      --  Gets time of next file commit

   private
      File_Commit_Time : Time := On_The_Hour (Clock) + File_Commit_Interval;
      --  Once per hour on the hour, first commit could beless than one hour
      --  after start
   end File_Commit_Times;

   protected body File_Commit_Times is

      procedure Set_Next_File_Commit is

         --  Sets time for next file commit

      begin -- Set_Next_File_Commit
         File_Commit_Time := File_Commit_Time + File_Commit_Interval;
      end Set_Next_File_Commit;

      function Get_Next_File_Commit return Time is

         --  Gets time of next file commit

      begin --  Get_Next_File_Commit
         return File_Commit_Time;
      end Get_Next_File_Commit;

   end File_Commit_Times;

   Logging_File_Commit_Time : File_Commit_Times;

   function Read_File_Commit_Time return Time is

      --  Returns time of next file commit, that is, Flush (xx)

   begin -- Read_File_Commit_Time
      return Logging_File_Commit_Time.Get_Next_File_Commit;
   end Read_File_Commit_Time;

   function Logging_Path (This_Time : Time) return string is

      -- Returns "YYYY" representing the current year

   begin -- Logging_Path
      return Reverse_Date_String (This_Time) (1 .. 4);
   end Logging_Path;

   function Logging_File_Name (This_Time : Time) return string is

      --  Returns "YYYYMMDD.csv" where YYYYMMDD represents the current date

   begin -- Logging_File_Name
      return '/' & Reverse_Date_String (This_Time) & ".csv";
   end Logging_File_Name;

   procedure Open_Log_File
     (Logging_File : in out File_Type; This_Time : in Time) is

   begin -- Open_Log_File
      if not (Exists (Logging_Path (This_Time))) then
         Create_Directory (Logging_Path (This_Time));
      --  Exceptions may be raised later if the name exists and is a file not
      --  a directory
      end if; -- not (Exists (Logging_Path (This_Time)))
      if Exists (Logging_Path (This_Time) & Logging_File_Name (This_Time)) then
         Open (Logging_File,
               Append_File,
               Logging_Path (This_Time) & Logging_File_Name (This_Time));
      else
         Create (Logging_File, Out_File,
                 Logging_Path (This_Time) & Logging_File_Name (This_Time));
         Put_Line (Logging_File, "Time,Message_Delay,Voltage,Current,"
                   & "Power,VA,VAR,Power_Factor,Frequency,"
                   & "Import_kWh,Export_kwh,Hours_Counter");
         --  Write header in logging file
      end if; -- Exists (Logging_Path (This_Time) & ...
   exception
      when E : others =>
         Put_Error ("Open_Log_File", E);
         raise;
   end Open_Log_File;

   function Is_Next_Day (Old_Time, New_Time : Time) return Boolean is

      Year : Year_Number;
      Month : Month_Number;
      Day, Next_Day : Day_Number;
      Seconds : Day_Duration;

   begin -- Is_Next_Day
      Split (Old_Time, Year, Month, Day, Seconds);
      Split (New_Time, Year, Month, Next_Day, Seconds);
      return Next_Day /= Day;
   end Is_Next_Day;

   function Calculate_Next_Log_Entry return Time is

      Year : Year_Number;
      Month : Month_Number;
      Day : Day_Number;
      Seconds : Day_Duration;
      This_Time : Time;

   begin -- Calculate_Next_Log_Entry
      This_Time := Clock;
      if Is_Next_Day (This_Time, This_Time + Log_Interval) then
         Split (This_Time + Log_Interval, Year, Month, Day, Seconds);
         return Ada.Calendar.Time_Of (Year, Month, Day, 0.0);
      else
         Split (This_Time, Year, Month, Day, Seconds);
         Seconds :=
           Duration (Integer (Seconds / Log_Interval)) * Log_Interval
           + Log_Interval;
         return Ada.Calendar.Time_Of (Year, Month, Day, Seconds);
      end if; -- Next_Day (This_Time, This_Time + Log_Interval)
   end Calculate_Next_Log_Entry;

   procedure Start_Logging
     (Logging_File : in out File_Type; Next_Log_Entry : out Time) is

   begin -- Start_Logging
      Next_Log_Entry := Calculate_Next_Log_Entry;
      Open_Log_File (Logging_File, Next_Log_Entry);
   end Start_Logging;

   procedure Put_Log_Entry (Logging_File : in out File_Type) is

      Prefix : String := "Put_Log_Entry - ";
      Delimeter : constant Character := ',';
      Log_Entry : Unbounded_String := Null_Unbounded_String;
      Item_String : String (1 .. 25);
      --  Allows for sign, 15 digits, decimal. exponent, etc.
      Log_Datum : Log_Data;
      Call_Time : Time;
      Days : Day_Count;
      Leap_Seconds : Leap_Seconds_Count;
      Message_Delay : Duration;

   begin -- Put_Log_Entry
      Call_Time := Clock;
      Receiver.Get (Log_Datum);
      Log_Entry := @ & Time_String (Call_Time) & Delimeter;
      Difference (Log_Datum.Message_Time, Call_Time, Days,
                  Message_Delay, Leap_Seconds);
      Duration_IO.Put (Item_String, Message_Delay, 1, 0);
      Log_Entry := @ & Trim (Item_String, Both) & Delimeter;
      Fixed_1_IO.Put (Item_String, Log_Datum.Voltage, 1, 0);
      Log_Entry := @ & Trim (Item_String, Both) & Delimeter;
      Fixed_3_IO.Put (Item_String, Log_Datum.Current, 3, 0);
      Log_Entry := @ & Trim (Item_String, Both) & Delimeter;
      Fixed_1_IO.Put (Item_String, Log_Datum.Power, 1, 0);
      Log_Entry := @ & Trim (Item_String, Both) & Delimeter;
      Fixed_1_IO.Put (Item_String, Log_Datum.VA, 1, 0);
      Log_Entry := @ & Trim (Item_String, Both) & Delimeter;
      Fixed_1_IO.Put (Item_String, Log_Datum.VAR, 1, 0);
      Log_Entry := @ & Trim (Item_String, Both) & Delimeter;
      Fixed_3_IO.Put (Item_String, Log_Datum.Power_Factor, 3, 0);
      Log_Entry := @ & Trim (Item_String, Both) & Delimeter;
      Fixed_1_IO.Put (Item_String, Log_Datum.Frequency, 1, 0);
      Log_Entry := @ & Trim (Item_String, Both) & Delimeter;
      Fixed_1_IO.Put (Item_String, Log_Datum.Import_kWh, 1, 0);
      Log_Entry := @ & Trim (Item_String, Both) & Delimeter;
      Fixed_1_IO.Put (Item_String, Log_Datum.Export_kWh, 1, 0);
      Log_Entry := @ & Trim (Item_String, Both) & Delimeter;
      Fixed_2_IO.Put (Item_String, Log_Datum.Hours_Counter, 2, 0);
      Log_Entry := @ & Trim (Item_String, Both);
      Put_Line (Logging_File, Log_Entry);
   exception
      when E : others =>
         Put_Error (Prefix & "unhandled exception", E);
         raise;
   end Put_Log_Entry;

   task body Logger is

      Run_Logger               : Boolean := True;
      Next_Time, Previous_Time : Time;

   begin -- Logger
      accept Start (Broker_Host, User_Name, Password, Topic : String) do
         Receiver.Start (Broker_Host, User_Name, Password, Topic);
         Start_Logging (Logging_File, Next_Time);
         Previous_Time := Next_Time - Log_Interval;
      end Start;
      while Run_Logger loop
         select
            accept Stop do
               Run_logger := False;
               Close (Logging_File);
               Receiver.Stop;
            end Stop;
         or
            delay until Next_Time;
            if Clock - Next_Time > 2 * Log_Interval then
               -- a big step forward in time has occurred
               Next_Time := Calculate_Next_Log_Entry;
            end if; -- Next_Time - Clock > Log_Interval
            if Is_Next_Day (Previous_Time, Next_Time) then
               -- Day has changed hence a new logging file is required
               Close (Logging_File);
               Previous_Time := Next_Time;
               -- N.B. Start_Logging updates Next_Time!
               Start_Logging (Logging_File, Next_Time);
            else
               Previous_Time := Next_Time;
               Next_Time := Next_Time + Log_Interval;
            end if; -- Is_Next_Day (Previous_Time, Next_Time)
            Put_Log_Entry (Logging_File);
            if Clock >= Logging_File_Commit_Time.Get_Next_File_Commit then
               Flush (Logging_File);
               Logging_File_Commit_Time.Set_Next_File_Commit;
            end if; -- Clock >= File_Commit_Time
         end select;
      end loop;
   exception
      when E : others =>
         Put_Error ("Logger", E);
         Close (Logging_File);
         Receiver.Stop;
         raise;
   end Logger;

end Data_Logger;
